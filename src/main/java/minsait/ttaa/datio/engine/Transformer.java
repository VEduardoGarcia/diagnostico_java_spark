package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.potential;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {
    private final SparkSession spark;

    @Value( "${param}" )
    private String param;

    public Transformer(@NotNull SparkSession spark)  {
        this.spark = spark;
        Dataset<Row> df = readInput();

        df = cleanData(df);
        df = addPlayerCat(df);
        df = addPotentialVsOverall(df);
        df = columnSelection(df);
        df = filterData(df);

        // for show 100 records after your transformations and show the Dataset schema
        df.show(100, false);
        df.printSchema();

        write(df);
    }

    /**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput() {
        return spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(INPUT_PATH);
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column nationality != null && column team_position != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        return df.filter(nationality.column().isNotNull()
                .and(teamPosition.column().isNotNull())
                .and(overall.column().isNotNull()));
    }

    /**
     * @param df is a Dataset with players information (must have nationality, team_position and overall columns)
     * @return add to the Dataset the column "player_cat"
     * by each position value
     * cat A for if is in 3 best players
     * cat B for if is in 5 best players
     * cat C for if is in 10 best players
     * cat D for the rest
     */
    private Dataset<Row> addPlayerCat(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(nationality.column(), teamPosition.column())
                .orderBy(overall.column());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(3), A)
                .when(rank.$less(5), B)
                .when(rank.$less(10), C)
                .otherwise(D);

        df = df.withColumn(playerCat.getName(), rule);

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have potential and overall columns)
     * @return add to the Dataset the column "potential_vs_overall"
     * potential column divided by the overall column
     */

    private Dataset<Row> addPotentialVsOverall(Dataset<Row> df) {

        Column rule = col(potential.getName()).divide(col(overall.getName()));

        return df.withColumn(potentialVsOverall.getName(),rule);

    }

    /**
     * @param df is a Dataset with players information
     * @return the columns used
     */
    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                playerCat.column(),
                potentialVsOverall.column()
        );
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * if player_cat is in the following values: A, B
     * if player_cat is C and potential_vs_overall is greater than 1.15
     * if player_cat is D and potential_vs_overall is greater than 1.25
     */
    private Dataset<Row> filterData(Dataset<Row> df) {
        df = df.filter(playerCat.column().isNotNull()
                .and(playerCat.column().isin(A,B,C,D))
                .and(when(playerCat.column().equalTo(C),col(potentialVsOverall.getName()).$greater(1.15))
                    .when(playerCat.column().equalTo(D),col(potentialVsOverall.getName()).$greater(1.25))
                    .otherwise(col(potentialVsOverall.getName()).$greater(0))
                ));
        return df;
    }
}