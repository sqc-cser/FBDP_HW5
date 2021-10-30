import java.io.*;
import java.net.URI;
import java.util.regex.Pattern;
import java.util.HashSet;
import java.util.Set;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.common.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;



public class WordCount {
    public static class SortReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
        //定义treeMap来保持统计结果,由于treeMap是按key升序排列的,这里要人为指定Comparator以实现倒排
        //这里先使用统计数为key，被统计的单词为value
        private TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>(new Comparator<Integer>() {
            @Override
            public int compare(Integer x, Integer y) {
                return y.compareTo(x);
            }
        });

       public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //reduce后的结果放入treeMap,而不是向context中记入结果
           int sum = 0;
           for (IntWritable val : values) {
               sum += val.get();
           }
           if (treeMap.containsKey(sum)) {  //具有相同单词数的单词之间用逗号分隔
               String value = treeMap.get(sum) + "," + key.toString();
               treeMap.put(sum, value);
           } else {
               treeMap.put(sum, key.toString());
           }
       }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            //将treeMap中的结果,按value-key顺序写入context中
            int times = 1;
            for (Integer key : treeMap.keySet()) {
                if(times<=100){
                    if (treeMap.get(key).toString().indexOf(",")!=-1) { // 说明有，有同样个数的单词
                        String[] splitstr=treeMap.get(key).toString().split(",");
                        for (int i=0;i<splitstr.length;++i){
                            if(times<=100){
                                context.write(new Text(splitstr[i]), new IntWritable(key));
                                times++;
                            }else{break;}
                        }
                    }
                    else{
                        String s = treeMap.get(key);
                        context.write(new Text(s),new IntWritable(key));
                        times++;
                    }
                }
            }
        }

    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{    // TokenizerMapper 继承 Mapper 父类
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private boolean caseSensitive = false;
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*"); // 正则表达式匹配，把标点符号split出来
        // \s 是匹配所有空白符，包括换行；
        // *匹配前面的子表达式零次或多次。例如，zo* 能匹配 "z" 以及 "zoo"。* 等价于{0,}。
        // \b匹配一个单词边界，即字与空格间的位置。
        private long numRecords = 0;
        private String input;  // 存储来自拆分文件的输入块
        private Set<String> patternsToSkip = new HashSet<String>();  // 从最终结果中删除的标点符号和多余单词的列表
        private Set<String> punctuations = new HashSet<String>(); // 用来保存所有要过滤的标点符号 stopwords
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            if (context.getInputSplit() instanceof FileSplit) {  // 将信息转换为字符串进行处理
                this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
            } else {
                this.input = context.getInputSplit().toString();
            }
            Configuration config = context.getConfiguration();
            this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
            if (config.getBoolean("wordcount.skip.patterns", false)) {
                // 如果系统变量 wordcount.skip.patterns 为真，
                // 则从分布式缓存文件中获取要跳过的模式列表，并将文件的 URI 转发到 parseSkipFile 方法
                URI[] localPaths = context.getCacheFiles();
                puncFile(localPaths[0]);
                parseSkipFile(localPaths[1]);
            }

        }
        private void puncFile(URI patternsURI) {
            try {
                BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
                String pattern;
                while ((pattern = fis.readLine()) != null) {
                    punctuations.add(pattern);
                }
            } catch (IOException ioe) {
                System.out.println("Caught exception while parsing the cached file '" + patternsURI + "' : " + StringUtils.stringifyException(ioe));
            }
        }
        private void parseSkipFile(URI patternsURI) {
            try {
                BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
                String pattern;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.out.println("Caught exception while parsing the cached file '" + patternsURI + "' : " + StringUtils.stringifyException(ioe));
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line  = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
            for (String pattern : punctuations) { // 将数据中所有满足patternsToSkip的pattern都过滤掉, replace by ""
                line = line.replaceAll(pattern, " ");
            }
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String curword = itr.nextToken();
                if (patternsToSkip.contains(curword) || curword.length()<3) {
                    continue;
                }
                word.set(curword);
                context.write(word,one);
            }
        }
    }
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    //文件合并的方法（传入要合并的文件路径）
    private static void joinFileDemo(ArrayList<String> src) {
        for(int i = 0; i < src.size(); i++) {
            File file = new File(src.get(i));
            StringBuffer sb = new StringBuffer();
            sb.append("total.txt");
            System.out.println(sb.toString());
            try {
                //读取小文件的输入流
                InputStream in = new FileInputStream(file);
                //写入大文件的输出流
                File file2 = new File(sb.toString());
                OutputStream out = new FileOutputStream(file2,true);
                int len = -1;
                byte[] bytes = new byte[10*1024*1024];
                while((len = in.read(bytes))!=-1) {
                    out.write(bytes, 0, len);
                }
                out.close();
                in.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("文件合并完成！");
    }


    public static void main(String[] args) throws Exception {
        ArrayList<String> list = new ArrayList<String>(0);//用arraylist保存扫描到的路径
        String path = args[0].toString()+"/shakespeare-txt";
        File file = new File(path);
        File[] files = file.listFiles();
        String[] filenames = file.list();
        if (filenames == null)
            return;
        for (int i = 0; i < filenames.length; i++) {
            if (files[i].isFile()) {
                if (files[i].getName().endsWith(".txt"))
                    list.add(files[i].getPath());//获取路径
            }
        }

        for(int i=0;i<list.size();++i){
            Configuration conf_new = new Configuration();
            Job job_new = Job.getInstance(conf_new, "word count");
            job_new.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            job_new.addCacheFile(new Path(args[0]+ "/skip/punctuation.txt").toUri());
            job_new.addCacheFile(new Path(args[0]+ "/skip/stop-word-list.txt").toUri());
            job_new.setJarByClass(WordCount.class);
            job_new.setMapperClass(TokenizerMapper.class);
            job_new.setCombinerClass(SortReducer.class);
            job_new.setReducerClass(SortReducer.class);
            job_new.setOutputKeyClass(Text.class);
            job_new.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job_new, new Path(list.get(i)));
            System.out.println(list.get(i));
            FileOutputFormat.setOutputPath(job_new, new Path(args[1]+"/"+list.get(i)));
            job_new.waitForCompletion(true);
        }

//        System.out.println(list); // 获取所有的文件
//        joinFileDemo(list);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
        job.addCacheFile(new Path(args[0]+ "/skip/punctuation.txt").toUri());
        job.addCacheFile(new Path(args[0]+ "/skip/stop-word-list.txt").toUri());
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(SortReducer.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]+"/total.txt"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/total"));
        job.waitForCompletion(true);
    }
}