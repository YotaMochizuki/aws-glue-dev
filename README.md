# aws-glue-dev
AWS Glue 実験用


## Githubリポジトリ設定

### SSHキーを作成してGithubにアップロード
```bash
# 生成コマンド
ssh-keygen -t rsa -b 4096 -C "your-email@example.com"
```

### .bash_profileへ下記を書き込み
``` bash
# 最初に SSH_AUTH_SOCK（ssh-agent のソケットパス）が未定義か確認
if [ -z "$SSH_AUTH_SOCK" ]; then
    # ssh-agent をバックグラウンドで起動し、その環境変数を現在のシェルに設定
    eval "$(ssh-agent -s)" > /dev/null
fi

# もう一度 SSH_AUTH_SOCK をチェック（上の eval に失敗した場合も考慮）
if [ -z "$SSH_AUTH_SOCK" ]; then
    # 現在実行中の ssh-agent プロセスが存在するかを確認
    RUNNING_AGENT="`ps -ax | grep 'ssh-agent -s' | grep -v grep | wc -l | tr -d '[:space:]'`"
    if [ "$RUNNING_AGENT" = "0" ]; then
        # ssh-agent が動作していない場合、新しく起動し、
        # その出力（環境変数定義）をファイルに保存
        ssh-agent -s &> $HOME/.ssh/ssh-agent
    fi

    # 保存された ssh-agent の環境変数定義を読み込む
    eval `cat $HOME/.ssh/ssh-agent` > /dev/null

    # 登録済みの SSH 秘密鍵がなければ、ssh-add により追加（エラーは無視）
    ssh-add 2> /dev/null
fi

```

---

### ~/.ssh/config を用意する（必要なら）
```conf
# ~/.ssh/config
Host github.com
  HostName github.com
  User git
  IdentityFile ~/.ssh/github
IdentitiesOnly yes
```