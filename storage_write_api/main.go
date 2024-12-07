package main

import (
	"context"
	"encoding/json"
	"os"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	mw "cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/m-mizutani/goerr"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func main() {
	projectID := os.Getenv("PROJECT_ID")
	datasetID := os.Getenv("DATASET_ID")
	tableID := os.Getenv("TABLE_ID")

	schema := bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "age", Type: bigquery.IntegerFieldType},
	}
	data := []any{
		map[string]any{"name": "Alice", "age": 20},
		map[string]any{"name": "Bob", "age": 30},
	}

	ctx := context.Background()
	if err := insert(ctx, projectID, datasetID, tableID, schema, data); err != nil {
		panic(err)
	}
}

// データを書き込む関数の本体です。
func insert(ctx context.Context, projectID, datasetID, tableID string, schema bigquery.Schema, data []any) error {
	// まずは、ManagedWriterクライアントを作成します。これはプロジェクトごとに作成します。
	mwClient, err := mw.NewClient(ctx, projectID)
	if err != nil {
		return goerr.Wrap(err, "failed to create bigquery client").With("projectID", projectID)
	}

	// 次に、BigQueryのスキーマをStorage Write  API用のスキーマに変換します。
	convertedSchema, err := adapt.BQSchemaToStorageTableSchema(schema)
	if err != nil {
		return goerr.Wrap(err, "failed to convert schema")
	}

	// さらに、Storage API用のスキーマから変換を行い、最終的にStorage Write APIのProtocol Buffer用のDescriptorを取得します。これを使ってStorage Write APIは送信するデータのスキーマを理解します。
	descriptor, err := adapt.StorageSchemaToProto2Descriptor(convertedSchema, "root")
	if err != nil {
		return goerr.Wrap(err, "failed to convert schema to descriptor")
	}
	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return goerr.Wrap(err, "adapted descriptor is not a message descriptor")
	}
	descriptorProto, err := adapt.NormalizeDescriptor(messageDescriptor)
	if err != nil {
		return goerr.Wrap(err, "failed to normalize descriptor")
	}

	// ここから、データを書き込むためのストリームを作成します。今回はバッチ処理に適したPendingStreamを使います。
	ms, err := mwClient.NewManagedStream(ctx,
		mw.WithDestinationTable(mw.TableParentFromParts(projectID, datasetID, tableID)),
		mw.WithType(mw.PendingStream),
		mw.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		return goerr.Wrap(err, "failed to create managed stream")
	}
	defer ms.Close()

	// AppendRows は最大10MBまでのデータを一度に送信できます。厳密に送信データ量を制御することもできますが、ここではざっくりと100行ずつ送信することにします。
	const maxRows = 100

	// Storage Write APIにデータを送信するためには、ユーザ自身でバイト列までデータを変換する必要があります。データの詳しい変換方法は convertDataToBytes を参照してください。
	for s := 0; s < len(data); s += maxRows {
		e := min(s+maxRows, len(data))
		rows, err := convertDataToBytes(messageDescriptor, data[s:e])
		if err != nil {
			return goerr.Wrap(err, "failed to convert data to bytes")
		}

		// AppendRows でデータを送信します。ここではまだBigQueryにデータが書き込まれていないことに注意してください。
		resp, err := ms.AppendRows(ctx, rows)
		if err != nil {
			return goerr.Wrap(err, "failed to append rows")
		}
		// AppendRows は非同期で動作するため、結果を取得する必要があります。ここではエラーがあればそれを返します。
		if _, err := resp.GetResult(ctx); err != nil {
			return goerr.Wrap(err, "failed to wait append rows")
		}
	}

	// 送信するべきデータを全て AppendRows で送信し終えたら、最後に Finalize を呼び出してデータがこれ以上書き込まれないことを通知します。
	if _, err := ms.Finalize(ctx); err != nil {
		return goerr.Wrap(err, "failed to finalize stream")
	}

	// 最後に、BatchCommitWriteStreams で書き込みを確定させます。ここまで完了してようやくBigQueryにデータが書き込まれます。BatchCommitWriteStreams は複数のストリームを一度に処理できますが、今回は1つだけ対応します。
	req := &storagepb.BatchCommitWriteStreamsRequest{
		Parent:       mw.TableParentFromStreamName(ms.StreamName()),
		WriteStreams: []string{ms.StreamName()},
	}
	resp, err := mwClient.BatchCommitWriteStreams(ctx, req)
	if err != nil {
		return goerr.Wrap(err, "failed to commit write streams")
	}

	// エラーがあればそれを返します。エラーがなければ成功です。
	if len(resp.GetStreamErrors()) > 0 {
		return goerr.Wrap(err, "failed to commit write streams").With("errors", resp.GetStreamErrors())
	}

	return nil
}

// これは任意の形式のデータ列をプロトコルバッファ用のバイト列に変換する関数です。
func convertDataToBytes(md protoreflect.MessageDescriptor, data []any) ([][]byte, error) {
	// 返り値はバイト列のスライスです。1件ずつデータを変換していきます。
	var rows [][]byte
	for _, v := range data {
		// まず、message descriptor から新しい メッセージの構造体を作成します。
		message := dynamicpb.NewMessage(md)

		// 次にデータを一度JSONに変換します。これはプロトコルバッファのメッセージに変換するための中間形式です。
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, goerr.Wrap(err, "failed to Marshal json message").With("v", v)
		}

		// そしてJSON形式のデータをプロトコルバッファのメッセージに変換します。
		err = protojson.Unmarshal(raw, message)
		if err != nil {
			return nil, goerr.Wrap(err, "failed to Unmarshal json message").With("raw", string(raw))
		}

		// 最後に、プロトコルバッファのメッセージをバイト列に変換します。
		b, err := proto.Marshal(message)
		if err != nil {
			return nil, goerr.Wrap(err, "failed to Marshal proto message")
		}

		// これで1件分のデータがバイト列に変換されました。これを返り値に追加します。
		rows = append(rows, b)
	}

	return rows, nil
}
