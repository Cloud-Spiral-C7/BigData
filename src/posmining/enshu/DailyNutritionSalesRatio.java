package posmining.enshu;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

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

/**
 * ある店舗立地における、全商品に対する栄養飲食料品の日ごとの売上比率、および曜日ごとの売上比率を求めるクラス。
 * なお、比率は 0 <= r <= 1 を満たす。
 *
 * 店舗立地:日付 = 曜日:日ごとの売上比率:曜日ごとの売上比率
 */
public class DailyNutritionSalesRatio {
	/**
	 * POSデータを利用して、全商品に対する曜日ごとの栄養飲食料品の売上の比率を求める
	 * @param args
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		String inputPath = "posdata";
		String outputPath = "out/nutritionRatioByDayOfWeek";

		if (args.length > 0) {
			inputPath = args[0];
		}

		DailyNutritionSalesRatio runner = new DailyNutritionSalesRatio(inputPath, outputPath);
		runner.run();
	}

	private String inputPath, outPath;

	/**
	 * コンストラクタ
	 *
	 * @param inputPath 入力データへのファイルパス
	 * @param outPath 出力先のファイルパス
	 */
	public DailyNutritionSalesRatio(String inputPath, String outPath) {
		this.inputPath = inputPath;
		this.outPath = outPath;
	}

	/**
	 * 分析を開始する
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public void run() throws IOException, InterruptedException, ClassNotFoundException  {
		Job job = new Job(new Configuration());
		job.setJarByClass(DailyNutritionSalesRatio.class);       // ★このファイルのメインクラスの名前
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

		FileInputFormat.setInputPaths(job, new Path(this.inputPath));
		FileOutputFormat.setOutputPath(job, new Path(this.outPath));

		// 出力フォルダは実行の度に毎回削除する（上書きエラーが出るため）
		PosUtils.deleteOutputDir(this.outPath);

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
			String year = csv[PosUtils.YEAR];
			String month = csv[PosUtils.MONTH];
			String day = csv[PosUtils.DATE];
			String date = year + month + day;
			String week = csv[PosUtils.WEEK];
			String price = csv[PosUtils.ITEM_PRICE];
			String count = csv[PosUtils.ITEM_COUNT];

			context.write(new CSKV(location + ":" + week), new CSKV(date + ":" + category + ":" + price + ":" + count));
		}
	}

	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {
			long allItemTotal = 0;
			long nutritionItemTotal = 0;
			String[] keys = key.toString().split(":");
			String location = keys[0];
			String dayOfWeek = keys[1];
			Map<String, List<DailyItem>> allDailyItems = new Hashtable<String, List<DailyItem>>();

			for (CSKV value : values) {
				String[] innerValues = value.toString().split(":");
				String date = innerValues[0];
				String category = innerValues[1];
				int itemPrice = Integer.parseInt(innerValues[2]);
				int itemCount = Integer.parseInt(innerValues[3]);
				int itemTotal = itemPrice * itemCount;

				if (TargetItem.isTargetItem(category)) {
					nutritionItemTotal += itemTotal;
				}

				allItemTotal += itemTotal;

				// 連想配列を使って date と売上項目をマッピング
				if (!allDailyItems.containsKey(date)) allDailyItems.put(date, new ArrayList<DailyItem>());
				allDailyItems.get(date).add(new DailyItem(category, itemPrice, itemTotal));
			}

			// 全体売上に対する、ある店舗立地における曜日ごとの栄養飲食料品比率
			double ratioOfNutrition = (double)nutritionItemTotal / allItemTotal;

			for (Map.Entry<String, List<DailyItem>> e : allDailyItems.entrySet()) {
				String date = e.getKey();
				List<DailyItem> dailyItems = e.getValue();
				int dailyAllItemTotal = 0;
				int dailyNutritionItemTotal = 0;

				for (DailyItem dailyItem : dailyItems) {
					int itemTotal = dailyItem.itemPrice * dailyItem.itemCount;

					if (TargetItem.isTargetItem(dailyItem.category)) {
						dailyNutritionItemTotal += itemTotal;
					}

					dailyAllItemTotal += itemTotal;
				}

				double dailyRatioOfNutrition = (double) dailyNutritionItemTotal / dailyAllItemTotal;

				context.write(
						new CSKV(location + ":" + date),
						new CSKV(dayOfWeek + ":" + String.valueOf(dailyRatioOfNutrition) + ":" + String.valueOf(ratioOfNutrition)));
			}
		}
	}

	public static class DailyItem {
		public String category;
		public int itemPrice, itemCount;

		public DailyItem(String category, int itemPrice, int itemCount) {
			this.category = category;
			this.itemPrice = itemPrice;
			this.itemCount = itemCount;
		}
	}
}
