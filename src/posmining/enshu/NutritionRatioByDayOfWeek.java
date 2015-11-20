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

public class NutritionRatioByDayOfWeek {
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		NutritionRatioByDayOfWeek runner = new NutritionRatioByDayOfWeek(args);
		runner.run();
	}

	private String[] args;

	/**
	 * コンストラクタ
	 *
	 * @param args コマンドライン引数
	 */
	public NutritionRatioByDayOfWeek(String[] args) {
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
	public void run() throws IOException, InterruptedException, ClassNotFoundException  {
		Job job = new Job(new Configuration());
		job.setJarByClass(NutritionRatioByDayOfWeek.class);       // ★このファイルのメインクラスの名前
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
		String outputpath = "out/nutritionDrinkAverageSalesByDayOfWeek";     // ★MRの出力先
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

//			if (!TargetItem.isTargetItem(category)) return;

			String week = csv[PosUtils.WEEK];
			String price = csv[PosUtils.ITEM_PRICE];
			String count = csv[PosUtils.ITEM_COUNT];

			context.write(new CSKV(location + ":" + week), new CSKV(category + ":" + price + ":" + count));
		}
	}

	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {
			long allItemTotal = 0;
			long nutritionItemTotal = 0;

			for (CSKV value : values) {
				String[] splited = value.toString().split(":");
				String category = splited[0];
				int itemPrice = Integer.parseInt(splited[1]);
				int itemCount = Integer.parseInt(splited[2]);
				int itemTotal = itemPrice * itemCount;

				if (TargetItem.isTargetItem(category)) {
					nutritionItemTotal += itemTotal;
				}

				allItemTotal += itemTotal;
			}

			double ratioOfNutrition = (double)nutritionItemTotal / allItemTotal;
			context.write(key, new CSKV(ratioOfNutrition));
		}
	}
}
