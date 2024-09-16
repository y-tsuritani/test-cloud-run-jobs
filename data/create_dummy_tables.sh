# ダミーデータ用のCSVファイルを30個作成する
# ファイル名は、テーブル名と同じにする

# テーブル名は、以下の通り
# - dummy_01
# - dummy_02
# - dummy_03

# ファイル名は、以下の通り
# - dummy_01.csv
# - dummy_02.csv
# - dummy_03.csv
# - ...
# - dummy_30.csv

# ファイルの中身は、以下の通り
# - 1行目: id, name, age
# - 2行目: 1, name_1, 20
# - 3行目: 2, name_2, 20
# - ...
# - 1000行目: 1000, name_1000, 20

# 行数は、各ファイル1000行とする

# ファイルの作成
for i in {1..30}; do
  echo "id,name,age" > dummy_$(printf "%02d" $i).csv
  for j in {1..1000}; do
    echo "$j,name_$j,20" >> dummy_$(printf "%02d" $i).csv
  done
done

# CSVファイルをGCSにアップロード
gsutil -m cp dummy_*.csv gs://test-source-csv

# CSVファイルからテーブルを作成
for i in {1..30}; do
  bq load --source_format=CSV --skip_leading_rows=1 --autodetect TEST_DATA.dummy_$(printf "%02d" $i) gs://test-source-csv/dummy_$(printf "%02d" $i).csv
done
