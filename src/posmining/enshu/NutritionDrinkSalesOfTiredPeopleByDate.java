package posmining.enshu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import posmining.enshu.NutritionDrinkAverageCost.MyMapper;
import posmining.enshu.NutritionDrinkAverageCost.MyReducer;
import posmining.target.TargetItem;
import posmining.utils.CSKV;
import posmining.utils.PosUtils;

public class NutritionDrinkSalesOfTiredPeopleByDate {

	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {


		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(NutritionDrinkAverageCost.class);       // ★このファイルのメインクラスの名前
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("2015031");                   // ★自分の学籍番号

		// 入出力フォーマットをテキストに指定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// MapperとReducerの出力の型を指定
		job.setMapOutputKeyClass(CSKV.class);
		job.setMapOutputValueClass(CSKV.class);
		job.setOutputKeyClass(CSKV.class);
		job.setOutputValueClass(CSKV.class);

		// 入出力ファイルを指定
		String inputpath = "posdata";
		String outputpath = "out/nutritionDrinkSalesOfTiredPeopleByLocation";     // ★MRの出力先
		if (args.length > 0) {
			inputpath = args[0];
		}

		FileInputFormat.setInputPaths(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		// 出力フォルダは実行の度に毎回削除する（上書きエラーが出るため）
		PosUtils.deleteOutputDir(outputpath);

		// Reducerで使う計算機数を指定
		job.setNumReduceTasks(8);

		// MapReduceジョブを投げ，終わるまで待つ．
		job.waitForCompletion(true);
	}


	// Mapperクラスのmap関数を定義
	public static class MyMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String csv[] = value.toString().split(",");

			String location = csv[PosUtils.LOCATION];
			String category = csv[PosUtils.ITEM_CATEGORY_NAME];

			if (location == "駅前3" || location == "オフィス街1" || location == "オフィス街4" ||
					!TargetItem.isTargetItem(category)) return;

			String year = csv[PosUtils.YEAR];
			String month = csv[PosUtils.MONTH];
			String day = csv[PosUtils.DATE];
			String week = csv[PosUtils.WEEK];
			String isHoliday = csv[PosUtils.IS_HOLIDAY];
			String price = csv[PosUtils.ITEM_PRICE];
			String keyStr = year + "," + month + "," + day + "," + week + "," + isHoliday + "," + location;

			context.write(new CSKV(keyStr), new CSKV(price));
		}
	}


	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {
			int totalPrice = 0;

			for (CSKV value : values) {
				int price = Integer.parseInt(value.toString());
				totalPrice += price;
			}

			context.write(key, new CSKV(totalPrice));
		}
	}
}
