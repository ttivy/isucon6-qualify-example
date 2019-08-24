# isucon6-qualify-example

# requirements
## pip
  - pyahocorasick

# changed
## mysql
  - isuda と isutar を統合して star を isuda に移す
  - created_at カラムを全テーブルから削除
  - star テーブルの keyword, user_name を entry_id, user_id に変更
  - entry.updated_at にインデックスを貼る
