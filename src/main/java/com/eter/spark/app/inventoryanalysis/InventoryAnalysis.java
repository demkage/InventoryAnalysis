package com.eter.spark.app.inventoryanalysis;

import com.eter.spark.app.inventoryanalysis.transformer.LastSaleTransformer;
import com.eter.spark.app.inventoryanalysis.transformer.NormalizeScoreTransformer;
import com.eter.spark.app.inventoryanalysis.transformer.SalesCountTransformer;
import com.eter.spark.app.inventoryanalysis.transformer.ScoreTransformer;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tree.impl.RandomForest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by abosii on 5/24/2017.
 */
public class InventoryAnalysis {

    private static final Logger log = LoggerFactory.getLogger(InventoryAnalysis.class);


    public static void main(String[] args) {
        if (args.length < 1) {
            log.error("Can't find argument for model output");
            log.debug("Actual arguments length: " + args.length);
            log.info("Use <application-name> path/to/model");
            return;
        }

        String output = args[0];

        boolean useLinearRegression = Boolean.parseBoolean(args[1]);

        log.info("Set model output as: " + output);

        SparkSession session = new SparkSession.Builder()
                .appName("InventoryAnalysis")
                .config("spark.sql.hive.metastore.version", "3.0.0")
                .config("spark.sql.hive.metastore.jars", "/usr/local/hadoop/share/hadoop/yarn/*:" +
                        "/usr/local/hadoop/share/hadoop/yarn/lib/*:" +
                        "/usr/local/hadoop/share/mapreduce/lib/*:" +
                        "/usr/local/hadoop/share/hadoop/mapreduce/*:" +
                        "/usr/local/hadoop/share/hadoop/common/*:" +
                        "/usr/local/hadoop/share/hadoop/hdfs/*:" +
                        "/usr//local/hadoop/etc/hadoop:" +
                        "/usr/local/hadoop/share/hadoop/common/lib/*:" +
                        "/usr/local/hadoop/share/hadoop/common/*:" +
                        "/usr/local/hive/lib/*:")
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> dataset = session.sql("SELECT id, saledate, productid, customerid, price, promo " +
                "FROM sales");

        LastSaleTransformer lastSaleTransformer = new LastSaleTransformer();
        SalesCountTransformer salesCountTransformer = new SalesCountTransformer();
        ScoreTransformer scoreTransformer = new ScoreTransformer();
        NormalizeScoreTransformer normalizeScore = new NormalizeScoreTransformer();

        Pipeline dataTransformerPipeline = new Pipeline()
                .setStages(new PipelineStage[]{lastSaleTransformer, salesCountTransformer, scoreTransformer,
                 normalizeScore });

        OneHotEncoder lastSaleEncoder = new OneHotEncoder()
                .setInputCol("lastsale")
                .setOutputCol("lastSaleVec");

        OneHotEncoder saleCountEnconder = new OneHotEncoder()
                .setInputCol("sales")
                .setOutputCol("salesVec");

        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[] {"lastSaleVec", "salesVec"})
                .setOutputCol("features");

        LinearRegression lr = new LinearRegression()
                .setLabelCol("score")
                .setFeaturesCol("features");

        Pipeline modelGenerator = new Pipeline()
                .setStages(new PipelineStage[] {
                        lastSaleEncoder, saleCountEnconder, vectorAssembler, lr
                });


        Dataset<Row> transformedDataset = dataTransformerPipeline.fit(dataset).transform(dataset);

        transformedDataset.show(1000);

        PipelineModel pipelineModel = modelGenerator.fit(transformedDataset);

        Dataset<Row> result = pipelineModel.transform(dataset);
        result.show();

    }
}
