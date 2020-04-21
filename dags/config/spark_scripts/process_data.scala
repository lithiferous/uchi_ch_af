import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

val spark = SparkSession.builder().getOrCreate()
val schema = new StructType()
      .add("ts", TimestampType, true)
      .add("userId", StringType, true)
      .add("sessionId", ShortType, true)
      .add("page", StringType, true)
      .add("auth", StringType, true)
      .add("method", StringType, true)
      .add("status", ShortType, true)
      .add("level", StringType, true)
      .add("itemInSession", ShortType, true)
      .add("location", StringType, true)
      .add("userAgent", StringType, true)
      .add("lastName", StringType, true)
      .add("firstName", StringType, true)
      .add("registration", LongType, true)
      .add("gender", StringType, true)
      .add("artist", StringType, true)
      .add("song", StringType, true)
      .add("length", FloatType, true)

val df = spark.read.schema(schema).json("/data/event-data.json")

val _df = df.filter($"userId"!=="")
            .withColumn("_loc", split($"location", ", "))
            .withColumn("ts",regexp_replace($"ts", """\d{3}""", "20"))
            .select($"ts".cast(DateType).as("date"),
                    $"ts".cast(TimestampType),
                    df("userId").cast(ShortType),
                    df("sessionId").cast(ShortType),
                    df("page").cast(StringType),
                    df("auth").cast(StringType),
                    df("method").cast(StringType),
                    df("status").cast(ShortType),
                    df("level").cast(StringType),
                    df("itemInSession").cast(ShortType).as("itemId"),
                    $"_loc".getItem(0).as("city"),
                    $"_loc".getItem(1).as("state"),
                    df("userAgent").cast(StringType),
                    df("lastName").cast(StringType),
                    df("firstName").cast(StringType),
                    df("registration").cast(LongType).as("reg"),
                    df("gender").cast(StringType),
                    df("artist").cast(StringType),
                    df("song").cast(StringType),
                    df("length").cast(FloatType))

_df.repartition(1).write.parquet("/data/event-data.pq")
