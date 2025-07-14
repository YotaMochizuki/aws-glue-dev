# AWS Glue 5.0 (Python 3.10, Spark 3.4)
FROM public.ecr.aws/glue/aws-glue-libs:5 

# 作業ディレクトリを設定
WORKDIR /home/hadoop/workspace

# root 権限でJAR用ディレクトリ作成とダウンロード
USER root
RUN mkdir -p /jars && \
    curl -fL -o /jars/s3-tables-catalog-for-iceberg-runtime-0.1.4.jar \
    https://repo1.maven.org/maven2/software/amazon/s3tables/s3-tables-catalog-for-iceberg-runtime/0.1.4/s3-tables-catalog-for-iceberg-runtime-0.1.4.jar

# ユーザーを戻す
USER hadoop