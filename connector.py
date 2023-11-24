mongodb_uri = "mongodb://127.0.0.1/Atlan.spark-mongodb"
mongodb_database = "Atlan"
mongodb_collection = "spark-mongodb"


def kafka_to_mongodb_pipeline(spark, topic):
    try:
        df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic) \
            .load()
        
        df = df.selectExpr("CAST(value AS STRING)")

        def writeToMongoDB(batch_df, batch_id):

            print(f"Messages in Topic {topic}:\n")
            batch_df.show(truncate=False)

            batch_df.write.format("mongo") \
                .mode("append") \
                .option("uri", mongodb_uri) \
                .option("database", mongodb_database) \
                .option("collection", mongodb_collection) \
                .save()

        query = df.writeStream \
            .outputMode("append") \
            .foreachBatch(writeToMongoDB) \
            .start()

        query.awaitTermination()

    except Exception as e:
        print("Error occurred:", e)
        import traceback
        traceback.print_exc()
