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

public class NutritionDrinkSalesByDate {
	public static final String[] weeks = {"月", "火", "水", "木", "金", "土", "日"};
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		NutritionDrinkSalesByDate runner = new NutritionDrinkSalesByDate(args);
		runner.run("オフィス街1", "Office1");
		runner.run("オフィス街4", "Office4");
		runner.run("駅前3", "Station3");
		runner.run("ロードサイド1", "RoadSide1");
	}

	private String[] args;

	/**
	 * コンストラクタ
	 *
	 * @param args コマンドライン引数
	 */
	public NutritionDrinkSalesByDate(String[] args) {
		this.args = args;
	}

	/**
	 * 分析を開始する
	 *
	 * @param location 店舗立地
	 * @param key ファイル名に使用する店舗立地表記
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public void run(String location, String filename) throws IOException, InterruptedException, ClassNotFoundException  {
		Configuration conf = new Configuration();
		conf.set("location", location);

		Job job = new Job(conf);
		job.setJarByClass(NutritionDrinkSalesByDate.class);       // ★このファイルのメインクラスの名前
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
		String outputpath = "out/nutritionDrinkSalesByDate-" + filename;     // ★MRの出力先
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
			Configuration conf = context.getConfiguration();

			String location = csv[PosUtils.LOCATION];
			String category = csv[PosUtils.ITEM_CATEGORY_NAME];

			if (!location.equals(conf.get("location")) || !TargetItem.isTargetItem(category)) return;

			String year = csv[PosUtils.YEAR];
			String month = csv[PosUtils.MONTH];
			String day = csv[PosUtils.DATE];
			int week = Integer.parseInt(csv[PosUtils.WEEK]);
			String isHoliday = csv[PosUtils.IS_HOLIDAY];
			int price = Integer.parseInt(csv[PosUtils.ITEM_PRICE]);
			int count = Integer.parseInt(csv[PosUtils.ITEM_COUNT]);

			String keyStr =
					year + "/" + month + "/" + day +
					" (" + NutritionDrinkSalesByDate.weeks[week - 1] + ")\t" +
					isHoliday + "\t" + location;

			context.write(new CSKV(keyStr), new CSKV(price * count));
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
