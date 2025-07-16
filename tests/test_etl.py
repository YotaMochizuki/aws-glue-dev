from modules.etl import transform


def test_transform_adds_uppercase_column(sample_data):
    """Transform 関数が name_upper カラムを追加するかをテストする."""
    # fixture から受け取ったサンプル DataFrame に対して変換処理を実行
    result_df = transform(sample_data)

    # 結果を取得し、name_upper カラムが期待通りか検証
    result = result_df.collect()[0]
    assert result["name_upper"] == "ALICE"
