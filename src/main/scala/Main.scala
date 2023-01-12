import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, functions => T}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master(master = "local[6]")
      .appName(name = "Final")
      .config("spark.executor.cores", "6")
      .config("spark.executor.instances", "4")
      .config("spark.cores.max", "128")
      .config("spark.executor.memory", "15g")
      .config("spark.sql.shuffle.partitions", "1")

      .getOrCreate()
    import spark.implicits._
    //----------------------------------------------------------------------------------------------------------------
    val pDate1 = "2020-11-01"
    val pDate2 = "2020-11-02"
    val pDate3 = "2020-11-03"
    val pDate4 = "2020-11-04" // Общий параметр для запуска витрин
    // Для запуска витрин просто пишем: val pDateCommon = pDate1 или val pDateCommon = pDate2 и.т.д

    val pDateCommon = pDate4// Все витрины будут построены на дату pDate1

    //---------------------------------------------------------------------------------------------------------
    // Составим схему для таблиц сданными
    // Таблица клиентов
    val schemaClients = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("ClientId", DataTypes.IntegerType, false),
      DataTypes.createStructField("ClientName", DataTypes.StringType, false),
      DataTypes.createStructField("Type", DataTypes.StringType, true),
      DataTypes.createStructField("Form", DataTypes.StringType, true),
      DataTypes.createStructField("RegisterDate", DataTypes.DateType, false)))

    //Таблица счетов
    val schemaAccounts = StructType(Array[StructField](
      DataTypes.createStructField("AccountID", DataTypes.IntegerType, false),
      DataTypes.createStructField("AccountNum", DataTypes.StringType, false),
      DataTypes.createStructField("ClientId", DataTypes.IntegerType, false),
      DataTypes.createStructField("DateOpen", DataTypes.DateType, false)))

    //Таблица операций
    val schemaOperations = StructType(Array(
      StructField("AccountDB", IntegerType, false),
      StructField("AccountCR", IntegerType, false),
      StructField("DateOp", DateType, false),
      StructField("Amount", StringType, false),
      StructField("Currency", StringType, false),
      StructField("Comment", StringType, true)))

    //Таблица курсов валют
    val schemaRates = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("Currency", DataTypes.StringType, false),
      DataTypes.createStructField("Rate", StringType, false),
      DataTypes.createStructField("RateDate", DataTypes.DateType, false)))


    //--------------------------------------------------------------------------------------------------------

    //чтение файла Account.csv как датафрейм
    var account_df = spark.read
      .format(("csv"))
      .option("header", "True")
      .option("multiline", true)
      .option("sep", ";")
      .option("dateformat", "yyyy-MM-dd")
      .option("enforceSchema", true)
      .schema(schemaAccounts)
      .load("data/Account.csv")

    //чтение файла Clients.csv как датафрейм
    var сlients_df = spark.read
      .format(("csv"))
      .option("header", "True")
      .option("multiline", true)
      .option("sep", ";")
      .option("dateformat", "yyyy-MM-dd")
      .option("enforceSchema", true)
      .schema(schemaClients)
      .load("data/Clients.csv")

    //чтение файла Operation.csv как датафрейм
    var operation_df = spark.read
      .format(("csv"))
      .option("header", "true")
      .option("multiline", true)
      .option("sep", ";")
      .option("enforceSchema", true)
      .option("dateformat", "yyyy-MM-dd")
      .schema(schemaOperations)
      .load("data/Operation.csv")
      .withColumn("Amount", regexp_replace($"Amount", ",", "."))

    //чтение файла Rate.csv как датафрейм
    var rate_df = spark.read
      .format(("csv"))
      .option("header", "true")
      .option("multiline", true)
      .option("sep", ";")
      .option("enforceSchema", true)
      .option("dateformat", "yyyy-MM-dd")
      .schema(schemaRates)
      .load("data/Rate.csv")
      .withColumn("Rate", regexp_replace($"Rate", ",", "."))
      .dropDuplicates("Currency")




    //------------------------------------------------------------------------------------------------
    //объеденим таблицу клиентов с таблицей счетов

    val clients_Accounts = сlients_df.join(account_df, "ClientId")
    //.show()
    //к таблице клиентов и счетов добавляем таблицу операций

    val clients_Accounts_Op = operation_df
      .join(clients_Accounts.select($"ClientId".as("ClientDB"),
        $"AccountId".as("AccDB"),
        $"AccountNum".as("NumDB"),
        $"Type".as("TypeDB"),
        $"Form".as("FormDB")
      ),
        $"AccDB" === $"AccountDB" || $"AccDB" === $"AccountCR", "left")
      .join(clients_Accounts.select($"ClientId".as("ClientCR"),
        $"AccountId".as("AccCR"),
        $"AccountNum".as("NumCR"),
        $"Type".as("TypeCR"),
        $"Form".as("FormCR")
      ),
        $"AccCR" === $"AccountCR", "left")
    //.show()



    // джойним таблицы
    var clients_Accounts_Op_Rate = clients_Accounts_Op
      .join(rate_df, "Currency")
    //.show()


    // таблица с суммами в рублях
    clients_Accounts_Op_Rate = clients_Accounts_Op_Rate
      .withColumn("AmountRUB", round(col("Amount") * col("Rate"), 2))
      .where($"DateOp" === pDateCommon)
    //clients_Accounts_Op_Rate.show()

    //------------------------------------------------------------------------------
    //a) построение PaymentAmt //Сумма операций по счету, где счет клиента указан в дебете проводки
    clients_Accounts_Op_Rate.groupBy($"AccountDB", $"ClientDB", $"DateOp")
      .agg(round(sum($"AmountRub"), 2)
        .as("PaymentAmt"))
    //.show()
    //b) построение EnrollementAmt //Сумма операций по счету, где счет клиента указан в кредите проводки
    clients_Accounts_Op_Rate.groupBy($"AccDB".as("AccountID"), $"ClientDB".as("ClientId"), $"DateOp")
      .agg(round(sum(when($"AccDB" === $"AccountDB", $"AmountRub")), 2).as("PaymentAmt"),
        round(sum(when($"AccDB" === $"AccountCR", $"AmountRub")), 2).as("EnrollementAmt")
      )
    //.show()
    //c) построение TaxAmt   Сумму операций, где счет клиента указан в дебете, и счет кредита 40702
    clients_Accounts_Op_Rate.groupBy($"AccDB".as("AccountID"), $"ClientDB".as("ClientId"), $"DateOp")
      .agg(round(sum(when($"AccDB" === $"AccountDB", $"AmountRub").otherwise(0)), 2).as("PaymentAmt"),
        round(sum(when($"AccDB" === $"AccountCR", $"AmountRub").otherwise(0)), 2).as("EnrollementAmt"),
        round(sum(when($"AccDB" === $"AccountDB" && $"NumCR".startsWith("40702"), $"AmountRub").otherwise(0)), 2).as("TaxAmt")
      )
    //.show()
    //d) построение ClearAmt  Сумма операций, где счет клиента указан в кредите, и счет дебета  40802
    clients_Accounts_Op_Rate.groupBy($"AccDB".as("AccountID"), $"ClientDB".as("ClientId"), $"DateOp")
      .agg(round(sum(when($"AccDB" === $"AccountDB", $"AmountRub").otherwise(0)), 2).as("PaymentAmt"),
        round(sum(when($"AccDB" === $"AccountCR", $"AmountRub").otherwise(0)), 2).as("EnrollementAmt"),
        round(sum(when($"AccDB" === $"AccountDB" && $"NumCR".startsWith("40702"), $"AmountRub").otherwise(0)), 2).as("TaxAmt"),
        round(sum(when($"AccDB" === $"AccountCR" && $"NumDB".startsWith("40802"), $"AmountRub").otherwise(0)), 2).as("ClearAmt")
      )
    //.show()


    //-------------------------------------------------------------------------------------------
    // [Построение витрины данных] (corporate_account). Витрина 1(без списков 1 и 2)
    val corporate_payments = clients_Accounts_Op_Rate.groupBy($"AccDB".as("AccountID"),
      $"ClientDB".as("ClientId"),
      $"DateOp".as("CutoffDt"))
      .agg(round(sum(when($"AccDB" === $"AccountDB", $"AmountRub").otherwise(0)), 2).as("PaymentAmt"),
        round(sum(when($"AccDB" === $"AccountCR", $"AmountRub").otherwise(0)), 2).as("EnrollementAmt"),
        round(sum(when($"AccDB" === $"AccountDB" && $"NumCR".startsWith("40702"), $"AmountRub").otherwise(0)), 2).as("TaxAmt"),
        round(sum(when($"AccDB" === $"AccountCR" && $"NumDB".startsWith("40802"), $"AmountRub").otherwise(0)), 2).as("ClearAmt")
      )
    // corporate_payments.show()
    val path1 = "Vitrina/" + pDateCommon + "/corporate_payments"
    corporate_payments.write
      .format("parquet")
      .option("path", path1)
      .mode("Overwrite").save()


    //--------------------------------------------------------------------------------------------------------------
    //[Построение витрины данных] (corporate_account). Витрина 2


    val corporate_account = corporate_payments
      .join(clients_Accounts.select($"AccountNum",
        $"AccountID",
        $"DateOpen",
        $"ClientName"), "AccountID")
      .withColumn("TotalAmt", round($"PaymentAmt" + $"EnrollementAmt", 2))
      .select($"AccountID", $"AccountNum", $"DateOpen", $"ClientId", $"ClientName", $"TotalAmt", $"CutoffDt")
      .orderBy("AccountID", "CutoffDt")

    //corporate_account.show()
    val path2 = "Vitrina/" + pDateCommon + "/corporate_account"
    corporate_account.write
      .format("parquet")
      .option("path", path2)
      .mode("Overwrite").save()
    //    val path = "Vitrina/" + pDateCommon + "/corporate_payments"
    //    corporate_payments.write
    //      .format("csv")
    //      .option("path", path)
    //      .mode("Overwrite").save()
    //    corporate_account.write
    //      .partitionBy("CutoffDt")
    //      .mode("overwrite")
    //      .parquet("corporate_account")
    //    corporate_account.write
    //     .partitionBy("CutoffDt")
    //     .parquet("/Users/vit555/Project №4 Clients and accounts/corporate_info/_corporate_account_2020-11-04.parquet")

    //
    //    //----------------------------------------------------------------
    // [Построение витрины данных] (corporate_info). Витрина #3
    val corporate_info = corporate_account
      .join(clients_Accounts.select($"ClientId".as("Clid"),
        $"ClientName".as("ClName"),
        $"Type",
        $"Form",
        $"RegisterDate"), $"Clid" === $"ClientId", "left")


    corporate_info.select($"ClientId",
      $"ClientName",
      $"Type",
      $"Form",
      $"RegisterDate",
      $"TotalAmt",
      $"CutoffDt")
    //.show()
    val path3 = "Vitrina/" + pDateCommon + "/corporate_info"
    corporate_info.write
      .format("parquet")
      .option("path", path3)
      .mode("Overwrite").save()





  }
}