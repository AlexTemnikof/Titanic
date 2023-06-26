package org.example.service;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import javax.annotation.PostConstruct;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class AppService {

    private SparkConf conf;
    private JavaSparkContext context;
    public AppService(){
        this.conf = new SparkConf().setMaster("local").setAppName("Titanic");
        this.context = new JavaSparkContext(conf);
    }
    public void pipeline(){
        SparkSession spark = SparkSession.builder()
                .appName("Titanic")
                .config("spark.master", "local")
                .getOrCreate();

            // Создание объекта JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

            // Загрузка CSV файла в DataFrame
        Dataset<Row> trainData = spark.read()
                .option("header", true)
                .csv("C:\\Intellij Idea\\Titanic\\src\\main\\resources\\train.csv");

            // Подсчет количества пропущенных значений по столбцам
        Column[] columns = new Column[trainData.columns().length];
        for (int i = 0; i < trainData.columns().length; i++) {
            columns[i] = col(trainData.columns()[i]);
        }

        trainData.select(columns)
                .describe()
                .filter(col("summary").equalTo("count"))
                .show();

            // Фильтрация строк с пропущенными значениями в столбце "Embarked"
        Dataset<Row> nullEmbarked = trainData.filter(col("Embarked").isNull());
        nullEmbarked.show();

            // Получение строки с индексом 61
        Row row61 = trainData.filter(col("PassengerId").equalTo(61))
                .first();

            // Получение строки с индексом 829
        Row row829 = trainData.filter(col("PassengerId").equalTo(829)).first();
        System.out.println(row829);


        Dataset<Row> fullEmbarked = trainData.filter(col("Pclass").equalTo(1).and(col("Sex").equalTo("female")))
                .select("Embarked");
        fullEmbarked.show();

        // Создание столбца "Embarked_Imputed" с помощью функции when/otherwise
        Dataset<Row> trainDataWithImputed = trainData.withColumn("Embarked_Imputed",
                functions.when(col("Embarked").isNull(), 1)
                        .otherwise(0));
        trainDataWithImputed.show();

            // Заполнение пропущенных значений в столбце "Embarked" значением 'C'
        trainData = trainData.na().fill("C", new String[]{"Embarked"});
        trainData.show();


        //NAME

        Dataset<Row> full_Name = trainData.select("Name");

        Dataset<String> prefixes = full_Name.map((Row row) -> {
            String string = row.getString(0);
            return string.substring(string.indexOf(',') + 2, string.indexOf('.'));
        }, Encoders.STRING()).distinct();

        List<String> prefixList = prefixes.collectAsList();

            // Вывод префиксов
        for (String prefix : prefixList) {
            System.out.println(prefix);
        }

        List<String> titul1 = new ArrayList<>(Arrays.asList("Col", "Major", "Capt", "the Countess"));
        List<String> titul2 = new ArrayList<>(Arrays.asList("Lady", "Sir", "Mme"));

        trainData = trainData.withColumn("BoyOrWoman", lit(0));

            // Вставка нового столбца 'Titul' со значением 3 в индекс 4
        trainData = trainData.withColumn("Titul", lit(3));

        trainData = trainData.withColumn("BoyOrWoman",
                functions.when(
                                col("Name").contains("Master").or(col("Sex").equalTo("female")),
                                1)
                        .otherwise(col("BoyOrWoman")));


            // Вывод схемы DataFrame после вставки новых столбцов
        trainData.show();


        //FARE
            // Рассчитываем среднее значение
        double mean = trainData.select(functions.mean(functions.when(trainData.col("Fare").notEqual(0), trainData.col("Fare"))))
                .first()
                .getDouble(0);

            // Создаем новую колонку 'Fare_Imputed' в trainData
        trainData = trainData.withColumn("Fare_Imputed",
                functions.when(trainData.col("Fare").equalTo(0), functions.lit(0))
                        .otherwise(functions.lit(1)));

            // Заменяем значения 0 на среднее значение в колонке 'Fare' в trainData
        trainData = trainData.withColumn("Fare",
                functions.when(trainData.col("Fare").equalTo(0), mean)
                        .otherwise(trainData.col("Fare")));


        //AGE
        trainData = trainData.withColumn("Age_Imputed",
                functions.when(trainData.col("Age").isNull(), functions.lit(1))
                        .otherwise(functions.lit(0)));
        //women
        double mean_w_1 = trainData.filter("Sex = 'female' AND Pclass = 1")
                .select(functions.avg("Age"))
                .first()
                .getDouble(0);

        double mean_w_2 = trainData.filter("Sex = 'female' AND Pclass = 2")
                .select(functions.avg("Age"))
                .first()
                .getDouble(0);

        double mean_w_3 = trainData.filter("Sex = 'female' AND Pclass = 3")
                .select(functions.avg("Age"))
                .first()
                .getDouble(0);

            // Заменяем пропущенные значения в trainData
        trainData = trainData.withColumn("Age", functions.when(trainData.col("Sex").equalTo("female").and(trainData.col("Pclass").equalTo(1)).and(trainData.col("Age").isNull()), mean_w_1)
                .when(trainData.col("Sex").equalTo("female").and(trainData.col("Pclass").equalTo(2)).and(trainData.col("Age").isNull()), mean_w_2)
                .when(trainData.col("Sex").equalTo("female").and(trainData.col("Pclass").equalTo(3)).and(trainData.col("Age").isNull()), mean_w_3)
                .otherwise(trainData.col("Age")));
        //boys
            // Вычисляем среднее значение для мальчиков (Master) в trainData
        double mean_boys = trainData.filter(functions.expr("Name LIKE '%Master%'"))
                .select(functions.avg("Age"))
                .first()
                .getDouble(0);

            // Заменяем пропущенные значения для мальчиков (Master) в trainData
        trainData = trainData.withColumn("Age", functions.when(trainData.col("Name").contains("Master").and(trainData.col("Age").isNull()), mean_boys)
                .otherwise(trainData.col("Age")));

        //men
        double mean_m_1 = trainData.filter(functions.expr("Sex = 'male' AND Pclass = 1"))
                .select(functions.avg("Age"))
                .first()
                .getDouble(0);

        double mean_m_2 = trainData.filter(functions.expr("Sex = 'male' AND Pclass = 2"))
                .select(functions.avg("Age"))
                .first()
                .getDouble(0);

        double mean_m_3 = trainData.filter(functions.expr("Sex = 'male' AND Pclass = 3"))
                .select(functions.avg("Age"))
                .first()
                .getDouble(0);

            // Заменяем пропущенные значения для мужчин в каждом классе в trainData
        trainData = trainData.withColumn("Age", functions.when(trainData.col("Sex").equalTo("male")
                        .and(trainData.col("Pclass").equalTo(1))
                        .and(trainData.col("Age").isNull()), mean_m_1)
                .when(trainData.col("Sex").equalTo("male")
                        .and(trainData.col("Pclass").equalTo(2))
                        .and(trainData.col("Age").isNull()), mean_m_2)
                .when(trainData.col("Sex").equalTo("male")
                        .and(trainData.col("Pclass").equalTo(3))
                        .and(trainData.col("Age").isNull()), mean_m_3)
                .otherwise(trainData.col("Age")));

        trainData.show();
        // Закрытие SparkSession и JavaSparkContext
        spark.close();
        sc.close();
    }

    private void cleanup(){

    }
    private void analyze(){

    }
}
