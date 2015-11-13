package posmining.enshu;

import static org.mockito.Matchers.intThat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import posmining.target.TargetItem;
import posmining.utils.CSKV;
import posmining.utils.PosUtils;

/**
 * 全おにぎりの販売個数を出力する
 * @author shin
 *
 */
public class NutritionDrinkAverageCost {


	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {


		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(NutritionDrinkAverageCost.class);       // ★このファイルのメインクラスの名前
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("2015036");                   // ★自分の学籍番号

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
		String outputpath = "out/nutritionDrinkAverageCost";     // ★MRの出力先
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

			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split(",");

			// TargetItemでないレシートは無視
			if (!TargetItem.isTargetItem(csv[PosUtils.ITEM_CATEGORY_NAME])) {
				return;
			}

			//keyとなる店舗立地を取得
			String location = csv[PosUtils.LOCATION];
			//valueとなる個数を取得
			String count = csv[PosUtils.ITEM_COUNT];
			//valueとなる販売単価を取得
			String price = csv[PosUtils.ITEM_PRICE];

			// emitする （emitデータはCSKVオブジェクトに変換すること）
			context.write(new CSKV(location), new CSKV(count + ":" +price));
		}
	}


	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {

			// 売り上げを合計
			double totalCount = 0.0;
			int totalPrice = 0;
			for (CSKV value : values) {
				String[] splitValue = value.toString().split(":");
				int count = Integer.parseInt(splitValue[0]);
				totalCount += count;
				totalPrice += Integer.parseInt(splitValue[1]) * count;
			}

			// emit
			context.write(key, new CSKV(String.format("%.2f", totalPrice/totalCount)));
		}
	}
}
