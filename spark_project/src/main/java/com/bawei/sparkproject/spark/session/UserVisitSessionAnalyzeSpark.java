package com.bawei.sparkproject.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.bawei.sparkproject.dao.*;
import com.bawei.sparkproject.domain.*;
import com.google.common.base.Optional;
import com.bawei.sparkproject.conf.ConfigurationManager;
import com.bawei.sparkproject.constant.Constants;
import com.bawei.sparkproject.dao.factory.DAOFactory;
import com.bawei.sparkproject.test.MockData;
import com.bawei.sparkproject.util.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/***
 *  * 用户访问session分析Spark作业
 *  *
 *  * 接收用户创建的分析任务，用户可能指定的条件如下：
 *  *
 *  * 1、时间范围：起始日期~结束日期
 *  * 2、性别：男或女
 *  * 3、年龄范围
 *  * 4、职业：多选
 *  * 5、城市：多选
 *  * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 *  * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 *
 *我们的spark作业如何接受用户创建的任务？
 *  *
 *  * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param
 *  * 字段中
 *  *
 *  * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
 *  * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
 *  * 参数就封装在main函数的args数组中
 *
 * 用户访问seesion分析Spark作业
 * @author Zhi-jiang li
 * @date 2020/2/2 0002 19:51
 **/
public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {
        args = new String[]{"1"};
        //构建Spark上下文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local")
//                .set("spark.default.parallelism","100")
                .set("spark.storage.memoryFraction", "0.5")
                .set("spark.shuffle.consolidateFiles", "true")
                .set("spark.shuffle.file.buffer", "64")
                .set("spark.shuffle.memoryFraction", "0.3")
                .set("spark.reducer.maxSizeInFlight","24")
                .set("spark.shuffle.io.maxRetries","60")
                .set("spark.shuffle.io.retryWait","60")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{
                        CategorySortKey.class,
                        IntList.class});

        /**
         * 比如,获取top10热门品类功能中,二次排序,自定义了一个key
         * 那个key是需要进行shuffle的时候,进行网络传输的,因此也实现序列化的
         * 启用Kyro机制以后,就会用Kyro去序列化和反序列化CategorySortKey
         * 所以这里要求,为了获取最佳性能,注册一下我们自定义的类
         */

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(sc.sc());

        //生成模拟测试数据
        mockData(sc, sqlContext);

        //创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        //首先得查询出指定的任务
        Long taskid = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDAO.findById(taskid);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //如果要进行session粒度的数据聚合,
        // 首先要从user_visit_action表中,查询指定日期内的数据

        /**
         * actionRDD,就是一个公共的RDD
         * 第一,要用actionRDD,获取到一个公共的sessionid为key的PairRDD
         * 第二,actionRDD,用在了session聚合环节里面
         *
         * <sessionid,Row>的RDD,是确定了,在后面要多次使用的
         * 1.与通过筛选的sessionid进行join,获取通过筛选的session的明细数据
         * 2.将这个RDD,直接传入aggregateBySession方法,进行session聚合统计
         *
         * 重构完以后,actionRDD,就只在最开始,使用一次,用来生成以sessionid为key的RDD
         */
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);

        //格式为<sessionid,row>
        JavaPairRDD<String, Row> sessionid2ActionRdd = getSessionid2ActionRdd(actionRDD);

        /**
         * 持久化，很简单，就是对RDD调用persist()方法，并传入一个持久化级别
         *
         * 如果是persist(StorageLevel.MEMORY_ONLY())，纯内存，无序列化，那么就可以用cache()方法来替代
         * StorageLevel.MEMORY_ONLY_SER()，第二选择
         * StorageLevel.MEMORY_AND_DISK()，第三选择
         * StorageLevel.MEMORY_AND_DISK_SER()，第四选择
         * StorageLevel.DISK_ONLY()，第五选择
         *
         * 如果内存充足，要使用双副本高可靠机制
         * 选择后缀带_2的策略
         * StorageLevel.MEMORY_ONLY_2()
         *
         */
        sessionid2ActionRdd = sessionid2ActionRdd.persist(StorageLevel.MEMORY_ONLY());

        // 首先，可以将行为数据，按照session_id进行groupByKey分组
        // 此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
        // 与用户信息数据，进行join
        // 然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user的信息

        JavaPairRDD<String, String> sessionid2AggInfoRDD =
                aggregateBySession(sqlContext, sessionid2ActionRdd);


        //接着,就要针对session粒度的聚合数据,按照使用者指定的筛选参数进行数据过滤
        //我们相当于自己编写的算子,是要访问外面的任务参数对象的
        //匿名内部类(算子函数),是要用fianl修饰的

        //重构,同时进行过滤和统计
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
                "", new SessionAggrStatAccumulator());


        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filtersessionAndAggrStat(
                sessionid2AggInfoRDD, taskParam, sessionAggrStatAccumulator);
        filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());


        //生成公共的RDD,通过筛选条件的session访问明细RDD
        /**
         * 重构:sessionid2detailRDD ,就是代表了通过筛选的session对应的访问明细数据
         */
        JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(
                filteredSessionid2AggrInfoRDD, sessionid2ActionRdd);
        sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());

        /**
         * 对于Accumulator这种分布式累加计算的变量的使用,有一个重要说明
         *
         * 从Accumulator中,获取数据,插入数据库的时候,一定要,一定要,是在有某一个action操作后
         * 再进行
         *
         * 如果没有action算子,那么整个程序根本不会运行
         *
         * 是不是在calculateAndPersistAggrStat方法之后,运行一个action操作,这是不对的
         *
         * 必须把能够触发job执行的操作,放在最终写入MYSQL方法之前
         */

        randomExtractSession(
                sc,
                task.getTaskid(),
                filteredSessionid2AggrInfoRDD,
                sessionid2detailRDD);

        /**
         * 特别声明
         * 我们知道,要将一个功能的session聚合统计数据获取到,就必须是在一个action操作触发job之后
         * 才能从Accumulator中获取数据,否则是 获取不到数据的,因为没有job执行,Accumulator的值为空
         * 所以,我们在这里,将随机抽取的功能的实现代码,放在session聚合统计功能的最终计算和写库之前
         * 因为随机抽取功能中,有一个countByKey算子,是action操作,会触发Job
         */

        //计算出各个范围的session占比,并写入MYSQL
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),
                task.getTaskid());

        /**
         * session聚合统计（统计出访问时长和访问步长，各个区间的session数量占总session数量的比例）
         *
         * 如果不进行重构，直接来实现，思路：
         * 1、actionRDD，映射成<sessionid,Row>的格式
         * 2、按sessionid聚合，计算出每个session的访问时长和访问步长，生成一个新的RDD
         * 3、遍历新生成的RDD，将每个session的访问时长和访问步长，去更新自定义Accumulator中的对应的值
         * 4、使用自定义Accumulator中的统计值，去计算各个区间的比例
         * 5、将最后计算出来的结果，写入MySQL对应的表中
         *
         * 普通实现思路的问题：
         * 1、为什么还要用actionRDD，去映射？其实我们之前在session聚合的时候，映射已经做过了。多此一举
         * 2、是不是一定要，为了session的聚合这个功能，单独去遍历一遍session？其实没有必要，已经有session数据
         * 		之前过滤session的时候，其实，就相当于，是在遍历session，那么这里就没有必要再过滤一遍了
         *
         * 重构实现思路：
         * 1、不要去生成任何新的RDD（处理上亿的数据）
         * 2、不要去单独遍历一遍session的数据（处理上千万的数据）
         * 3、可以在进行session聚合的时候，就直接计算出来每个session的访问时长和访问步长
         * 4、在进行过滤的时候，本来就要遍历所有的聚合session信息，此时，就可以在某个session通过筛选条件后
         * 		将其访问时长和访问步长，累加到自定义的Accumulator上面去
         * 5、就是两种截然不同的思考方式，和实现方式，在面对上亿，上千万数据的时候，甚至可以节省时间长达
         * 		半个小时，或者数个小时
         *
         * 开发Spark大型复杂项目的一些经验准则：
         * 1、尽量少生成RDD
         * 2、尽量少对RDD进行算子操作，如果有可能，尽量在一个算子里面，实现多个需要做的功能
         * 3、尽量少对RDD进行shuffle算子操作，比如groupByKey、reduceByKey、sortByKey（map、mapToPair）
         * 		shuffle操作，会导致大量的磁盘读写，严重降低性能
         * 		有shuffle的算子，和没有shuffle的算子，甚至性能，会达到几十分钟，甚至数个小时的差别
         * 		有shfufle的算子，很容易导致数据倾斜，一旦数据倾斜，简直就是性能杀手（完整的解决方案）
         * 4、无论做什么功能，性能第一
         * 		在传统的J2EE或者.NET后者PHP，软件/系统/网站开发中，我认为是架构和可维护性，可扩展性的重要
         * 		程度，远远高于了性能，大量的分布式的架构，设计模式，代码的划分，类的划分（高并发网站除外）
         *
         * 		在大数据项目中，比如MapReduce、Hive、Spark、Storm，我认为性能的重要程度，远远大于一些代码
         * 		的规范，和设计模式，代码的划分，类的划分；大数据，大数据，最重要的，就是性能
         * 		主要就是因为大数据以及大数据项目的特点，决定了，大数据的程序和项目的速度，都比较慢
         * 		如果不优先考虑性能的话，会导致一个大数据处理程序运行时间长度数个小时，甚至数十个小时
         * 		此时，对于用户体验，简直就是一场灾难
         *
         * 		所以，推荐大数据项目，在开发和代码的架构中，优先考虑性能；其次考虑功能代码的划分、解耦合
         *
         * 		我们如果采用第一种实现方案，那么其实就是代码划分（解耦合、可维护）优先，设计优先
         * 		如果采用第二种方案，那么其实就是性能优先
         *
         * 		讲了这么多，其实大家不要以为我是在岔开话题，大家不要觉得项目的课程，就是单纯的项目本身以及
         * 		代码coding最重要，其实项目，我觉得，最重要的，除了技术本身和项目经验以外；非常重要的一点，就是
         * 		积累了，处理各种问题的经验
         *
         */


        //获取Top10热门品类
        List<Tuple2<CategorySortKey, String>> top10CategoryList =
                getTop10Category(sessionid2detailRDD, task.getTaskid());

        //获取Top10活跃session
        getTop10Session(sc, task.getTaskid(), sessionid2detailRDD, top10CategoryList);

        //关闭Spark上下文
        sc.close();
    }


    /**
     * 如果是本地测试环境则生成SQLContext
     * 如果是生产环境运行这属实HiveContext
     *
     * @param sc
     * @return
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
     * 生成模拟数据(只有本地模式,才会生成)
     *
     * @param javaSparkContext
     * @param sqlContext
     */
    private static void mockData(
            JavaSparkContext javaSparkContext, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

        if (local) {
            MockData.mock(javaSparkContext, sqlContext);
        }
    }

    /**
     * 获取指定日期内的用户访问行为数据
     *
     * @param sqlContext SQLContext
     * @param taskParam  任务参数
     * @return 行为数据RDD
     */
    private static JavaRDD<Row> getActionRDDByDateRange(
            SQLContext sqlContext, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql = "select * " +
                "from user_visit_action " +
                "where date>='" + startDate + "' and " +
                "date<='" + endDate + "'";

        DataFrame actionDF = sqlContext.sql(sql);

        /**
         * 这里就很有可能发生上面说的问题
         * 比如说,Spark SQL默认就给第一个stage设置了20个task,但是根据你的数据量以及算法的复杂度
         * 实际上,你需要1000个task去执行
         *
         * 所以说,在这里,就可以对Spark SQL刚刚查询出来的RDD执行repartition重分区操作
         */
//      return actionDF.javaRDD().repartition(1000);
        return actionDF.javaRDD();
    }

    /**
     * 获取sessionid2到访问行为数据的映射的RDD
     * <sessionid,Row>格式
     *
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionid2ActionRdd(
            JavaRDD<Row> actionRDD) {
/*        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row)
                    throws Exception {

                return new Tuple2<String, Row>(row.getString(2), row);
            }
        });*/
        return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> rowIterator) throws Exception {
                List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    list.add(new Tuple2<String, Row>(row.getString(2), row));
                }
                return list;
            }
        });
    }


    /**
     * 对行为数据按session粒度进行聚合
     * 最终的格式<sessionid,fullAggrInfo<partAggInfo(sessionid,searchKeywords,clickCategoryIds,visitLength,stepLength,startTime),age,professional,city,sex>>
     *
     * @param sessionid2ActionRDD 行为数据RDD
     * @return session粒度聚合数据
     */
    private static JavaPairRDD<String, String> aggregateBySession(
            SQLContext sqlContext,
            final JavaPairRDD<String, Row> sessionid2ActionRDD) {

        //对行为数据按session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey();

        //对每一个session分组进行聚合,将session中所有的搜索词和点击品类都聚合起来
        //到此为止,获取的数据格式,如下<userid,partAggInfo(sessionid,searchKeywords,clickCategoryIds,visitLength,stepLength,startTime)>
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple)
                            throws Exception {
                        String sessionid = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        StringBuffer searchKeywordsBuffer = new StringBuffer("");
                        StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                        Long userid = null;

                        //session的开始和结束
                        Date startTime = null;
                        Date endTime = null;
                        //session的访问步长
                        int stepLength = 0;

                        //遍历session所有的访问行为
                        while (iterator.hasNext()) {
                            //提取每个访问行为的搜索字段和点击品类字段
                            Row row = iterator.next();
                            if (userid == null) {
                                userid = row.getLong(1);
                            }

                            String searchKeyword = row.getString(5);
                            Long clickCategoryId = row.getLong(6);

                            //其实,只有搜索行为,有searchKeyword字段
                            //只有点击品类行为,有clickCategoryId字段
                            //满足条件:不能是null值,其次之前的字符串中还没有搜索词或者点击品类id

                            if (StringUtils.isNotEmpty(searchKeyword)) {
                                if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                                    searchKeywordsBuffer.append(searchKeyword + ",");
                                }
                            }

                            if (clickCategoryId != null) {
                                if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
                                    clickCategoryIdsBuffer.append(clickCategoryId + ",");
                                }
                            }

                            //计算session开始合结束时间
                            Date actionTime = DateUtils.parseTime(row.getString(4));
                            if (startTime == null) {
                                startTime = actionTime;
                            }

                            if (endTime == null) {
                                endTime = actionTime;
                            }

                            if (actionTime.before(startTime)) {
                                startTime = actionTime;
                            }

                            if (actionTime.after(endTime)) {
                                endTime = actionTime;
                            }

                            //计算session访问步长
                            stepLength++;
                        }

                        String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                        //计算访问时长(秒)
                        long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

                        // 大家思考一下
                        // 我们返回的数据格式，即使<sessionid,partAggrInfo>
                        // 但是，这一步聚合完了以后，其实，我们是还需要将每一行数据，跟对应的用户信息进行聚合
                        // 问题就来了，如果是跟用户信息进行聚合的话，那么key，就不应该是sessionid
                        // 就应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合
                        // 如果我们这里直接返回<sessionid,partAggrInfo>，还得再做一次mapToPair算子
                        // 将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举

                        // 所以，我们这里其实可以直接，返回的数据格式，就是<userid,partAggrInfo>
                        // 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
                        // 然后再直接将返回的Tuple的key设置成sessionid
                        // 最后的数据格式，还是<sessionid,fullAggrInfo>

                        // 聚合数据，用什么样的格式进行拼接？
                        // 我们这里统一定义，使用key=value|key=value
                        String partAggInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                                + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                                + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                                + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                                + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                                + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);

                        return new Tuple2<Long, String>(userid, partAggInfo);
                    }
                });

        //查询所有用户数据 并映射为<userid,Row>格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();

        final JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        return new Tuple2<Long, Row>(row.getLong(0), row);
                    }
                });

        //将session粒度聚合数据,与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD =
                userid2PartAggrInfoRDD.join(userid2InfoRDD);


        //对join起来的数据进行拼接,并且返回<sessionid,(fullAggrInfo,row)>格式的数据
        JavaPairRDD<String, String> sessionid2FullAggInfoRDD = userid2FullInfoRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(
                            Tuple2<Long, Tuple2<String, Row>> tuple)
                            throws Exception {
                        String partAggInfo = tuple._2._1;
                        Row userInfoRow = tuple._2._2;

                        //获取sessionid
                        String sessionid = StringUtils.getFieldFromConcatString(
                                partAggInfo, "\\|", Constants.FIELD_SESSION_ID);

                        int age = userInfoRow.getInt(3);
                        String professional = userInfoRow.getString(4);
                        String city = userInfoRow.getString(5);
                        String sex = userInfoRow.getString(6);

                        String fullAggrInfo = partAggInfo + "|"
                                + Constants.FIELD_AGE + "=" + age + "|"
                                + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                                + Constants.FIELD_CITY + "=" + city + "|"
                                + Constants.FIELD_SEX + "=" + sex + "|";


                        return new Tuple2<String, String>(sessionid, fullAggrInfo);
                    }
                });
        return sessionid2FullAggInfoRDD;
    }


    /**
     * 过滤session数据,并进行聚合统计
     *
     * @return
     */
    private static JavaPairRDD<String, String> filtersessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggInfoRDD,
            final JSONObject taskParam,
            final Accumulator<String> sessionAggrStatAccumulator) {

        //为了使用我们后面的ValidUtils,所以,首先将所有的筛选参数拼接成一个连接串
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");

        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        //根据筛选参数进行过滤
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggInfoRDD.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        //首先从tuple中,获取聚合数据<sessionid,
                        // fullAggrInfo<partAggInfo(sessionid,searchKeywords,clickCategoryIds,visitLength,stepLength,startTime),age,professional,city,sex>>
                        String aggrInfo = tuple._2;

                        //接着,依次按照筛选条件进行过滤
                        //按照年龄范围进行过滤(startAge,endAge)
                        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                                parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }

                        //按照职业范围进行筛选(professional)
                        //互联网,IT,软件
                        //只要满足上面一个或者为空就满足
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
                                parameter, Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }

                        //按照城市进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                                parameter, Constants.PARAM_CITIES)) {
                            return false;
                        }

                        //按照男女进行过滤
                        //男/女
                        if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
                                parameter, Constants.PARAM_SEX)) {
                            return false;
                        }

                        //按照搜索词进行过滤
                        //主要判断session搜索词,有任何一个,与筛选条件相等则就符合
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
                                parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        //按照点击品类ID进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
                                parameter, Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        //如果经过之前多个过滤条件之后,程序能通过,
                        // 说明session是用户需要保留的session
                        //那么就要对session的访问时长和访问步长,进行统计,根据session对应的范围
                        //进行相应的累加计数

                        //只要走到这一步,那么就是需要计数的session
                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                        long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));

                        long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));

                        calculateVisitLength(visitLength);

                        calcuateStepLength(stepLength);


                        return true;
                    }

                    private void calculateVisitLength(long visitLength) {
                        if (visitLength >= 1 && visitLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        } else if (visitLength >= 4 && visitLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        } else if (visitLength >= 7 && visitLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        } else if (visitLength >= 10 && visitLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        } else if (visitLength > 30 && visitLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        } else if (visitLength > 60 && visitLength <= 180) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        } else if (visitLength > 180 && visitLength <= 600) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        } else if (visitLength > 600 && visitLength <= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if (visitLength > 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }

                    private void calcuateStepLength(long stepLength) {
                        if (stepLength >= 1 && stepLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        } else if (stepLength >= 4 && stepLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        } else if (stepLength >= 7 && stepLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        } else if (stepLength >= 10 && stepLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        } else if (stepLength > 30 && stepLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        } else if (stepLength > 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }

                    }
                });

        return filteredSessionid2AggrInfoRDD;
    }

    /**
     * //获取所有符合条件的session的访问明细<sessionId,Row>
     *
     * @param filteredSessionid2AggrInfoRDD
     * @param sessionid2ActionRdd
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionid2detailRDD(
            JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionid2ActionRdd) {
        //获取所有符合条件的session的访问明细<sessionId,Row>
        JavaPairRDD<String, Row> sessionid2detailRDD = filteredSessionid2AggrInfoRDD
                .join(sessionid2ActionRdd)
                .mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(
                            Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                        return new Tuple2<String, Row>(tuple._1, tuple._2._2);
                    }
                });
        return sessionid2ActionRdd;
    }

    /**
     * 随机抽取session
     *
     * @param taskid
     * @param Sessionid2AggrInfoRDD
     * @param sessionid2ActionRdd
     */
    private static void randomExtractSession(
            JavaSparkContext sc,
            final long taskid,
            //格式为<sessionid,
            //fullAggrInfo<partAggInfo(sessionid,searchKeywords,clickCategoryIds,visitLength,stepLength,startTime),age,professional,city,sex>>
            JavaPairRDD<String, String> Sessionid2AggrInfoRDD,
            //格式为<sessionid,row>
            JavaPairRDD<String, Row> sessionid2ActionRdd) {
        /**
         * 第一步,计算出每天每小时的session数量
         */

        //获取到<yyyy-mm-dd_HH,aggrInfo>格式的RDD
        JavaPairRDD<String, String> time2sessionidRDD = Sessionid2AggrInfoRDD.mapToPair(
                new PairFunction<Tuple2<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(
                            Tuple2<String, String> tuple) throws Exception {
                        String aggrInfo = tuple._2;
                        String startTime = StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_START_TIME);
                        //结果（yyyy-MM-dd_HH）
                        String dateHour = DateUtils.getDateHour(startTime);

                        return new Tuple2<String, String>(dateHour, aggrInfo);
                    }
                });

        /**
         * 每天每小时的session数量,然后计算出每天每小时的session抽取索引,遍历每天每小时session
         * 首先抽取出的session的聚合数据,写入session_random_extract表
         * 所以第一个RDD的value是Session的聚合数据
         */

        //得到每天每小时的session数量获取到<yyyy-mm-dd_HH,count>
        Map<String, Object> countMap = time2sessionidRDD.countByKey();

        //第二步,使用按时间比例抽取算法,计算每天每小时要抽取session的索引
        //将<yyyy-mm-dd_HH,count>格式的map,转换成<yyyy-mm-dd,<HH,count>>
        Map<String, Map<String, Long>> dateHourCountMap =
                new HashMap<String, Map<String, Long>>();
        for (Map.Entry<String, Object> countEntry : countMap.entrySet()) {
            String dateHour = countEntry.getKey();
            //yyyy-mm-dd
            String date = dateHour.split("_")[0];
            //HH
            String hour = dateHour.split("_")[1];

            long count = Long.valueOf(String.valueOf(countEntry.getValue()));

            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if (hourCountMap == null) {
                hourCountMap = new HashMap<String, Long>();
                dateHourCountMap.put(date, hourCountMap);
            }

            //把小时和每小时的session数量装进去
            hourCountMap.put(hour, count);
        }

        //开始实现我们的按时间比例随机抽取算法

        //总共要抽取100个session,先按照天数,进行平分
        //假如有十天,则每一天抽取的数量都为10%
        int extractNumberPerDay = 100 / dateHourCountMap.size();

        //<date,<hour,(3,2,20,102)>>
        /**
         * session随机抽取功能
         * 用到了一个比较大的变量,随机抽取索引的map
         * 之前是在算子里面使用了这个map,那么根据我们刚才讲的这个原理
         * 发现这个是比较消耗内存和网络传输性能的
         *
         * 将map做成广播变量
         */


        final Map<String, Map<String, List<Integer>>> dateHourExtractMap =
                new HashMap<String, Map<String, List<Integer>>>();
        Random random = new Random();

        //<yyyy-mm-dd,<HH,count>>现在格式
        for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

            //计算出这一天的session总数
            long sessionCount = 0L;
            for (long hourCount : hourCountMap.values()) {
                sessionCount += hourCount;
            }

            //先获取到这一天和他的List索引<HH,List>
            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<String, List<Integer>>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            //遍历每个小时的session总数
            for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();

                //计算每个小时的session数量,占据当天总seesion的数量的比例,直接乘以每天要抽取的数量
                //就可以算出,当前小时的需要抽取的session的数量
                int hourExtractNumber = (int) (((double) count / (double) sessionCount)
                        * extractNumberPerDay);

                if (hourExtractNumber > count) {
                    hourExtractNumber = (int) count;
                }

                //先获取当前小时的存放随机数的List
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if (extractIndexList == null) {
                    extractIndexList = new ArrayList<Integer>();
                    hourExtractMap.put(hour, extractIndexList);
                }

                //生成计算出来的数量的随机数
                for (int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt((int) count);
                    //不能重复
                    while (extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) count);
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        }

        /**
         * fastutil的使用,很简单,比如List<Integer>的List,对用的fastutil,就是Int
         */
        Map<String, Map<String, IntList>> fastutilDateHourExtractMap = new
                HashMap<String, Map<String, IntList>>();
        for (Map.Entry<String, Map<String, List<Integer>>> dateHourExtractEntry :
                dateHourExtractMap.entrySet()) {
            String date = dateHourExtractEntry.getKey();

            Map<String, List<Integer>> hourExtractMap = dateHourExtractEntry.getValue();
            Map<String, IntList> fastutilHourExtractMap = new HashMap<String, IntList>();

            for (Map.Entry<String, List<Integer>> hourExtractEntry : hourExtractMap.entrySet()) {
                String hour = hourExtractEntry.getKey();

                List<Integer> extractList = hourExtractEntry.getValue();
                IntList fastutilExtractList = new IntArrayList();
                for (int i = 0; i < extractList.size(); i++) {
                    fastutilExtractList.add(extractList.get(i));
                }

                fastutilHourExtractMap.put(hour, fastutilExtractList);
            }
            fastutilDateHourExtractMap.put(date, fastutilHourExtractMap);
        }

        /**
         * 广播变量.很简单,
         * 其实就是SparkContext的broadCast()方法,传入你要广播的变量,即可
         */
        final Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadcast =
                sc.broadcast(fastutilDateHourExtractMap);

        /**
         * 第三部:遍历每天每小时的session,然后根据随机索引进行抽取
         */

        //执行groupByKey算子,得到<yyyy-mm-dd_HH,(aggInfo)>
        JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD.groupByKey();

        //利用flatMap算子,遍历所有的<yyyy-mm-dd_HH,(aggInfo)>格式的数据
        //然后呢,会遍历每天每小时的session
        //如果发现某个session恰巧在我们指定的这天这小时的随机抽取索引上
        //那么抽取该session,直接写入MYSQL的random_extract_session表
        //将抽取出来的session_id返回回来,形成一个新的JavaPairRDD<String,String>
        //然后最后一步,是用抽取出来的session_id,去join他们的访问行文明细数据,写入session表
        JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(
                            Tuple2<String, Iterable<String>> tuple)
                            throws Exception {
                        List<Tuple2<String, String>> extractSessionids =
                                new ArrayList<Tuple2<String, String>>();

                        String dateHour = tuple._1;
                        String date = dateHour.split("_")[0];
                        String hour = dateHour.split("_")[1];

                        //得到aggInfo
                        Iterator<String> iterator = tuple._2.iterator();

                        /**
                         * 使用广播变量的时候
                         * 直接调用广播变量(Broadcast类型)学value()/getValue()
                         * 可以获取到之前封装的广播变量
                         */
                        Map<String, Map<String, IntList>> dateHourExtractMap =
                                dateHourExtractMapBroadcast.value();


                        //<date,<hour,(3,2,20,102)>>拿到了(3,2,20,102)这些数据
                        List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);

                        ISessionRandomExtract sessionRandomExtractDAO =
                                DAOFactory.getSessionRandomExtract();

                        int index = 0;
                        while (iterator.hasNext()) {
                            //格式<(aggInfo)>
                            String sessionAggrInfo = iterator.next();

                            //如果存在,则抽取出来
                            if (extractIndexList.contains(index)) {
                                String sessionid = (StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID));

                                //将数据写入Mysql
                                SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                                sessionRandomExtract.setTaskid(taskid);
                                sessionRandomExtract.setSessionid(sessionid);
                                sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
                                sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                                sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));

                                sessionRandomExtractDAO.insert(sessionRandomExtract);

                                //将sessionid加入list
                                extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));
                            }

                            index++;
                        }

                        return extractSessionids;
                    }
                });

        /**
         * 第四步:获取抽取出来的session的明细数据
         */
        JavaPairRDD<String, Tuple2<String, Row>> extractsessionDetailRDD =
                extractSessionidsRDD.join(sessionid2ActionRdd);

/*        extractsessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            @Override
            public void call(
                    Tuple2<String, Tuple2<String, Row>> tuple)
                    throws Exception {
                Row row = tuple._2._2;
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskid(taskid);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));

                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAL();

                sessionDetailDAO.insert(sessionDetail);

            }
        });*/

        extractsessionDetailRDD.foreachPartition(
                new VoidFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>>() {
                    @Override
                    public void call(
                            Iterator<Tuple2<String, Tuple2<String, Row>>> iterator)
                            throws Exception {
                        List<SessionDetail> sessionDetails = new ArrayList<SessionDetail>();

                        while (iterator.hasNext()) {
                            Tuple2<String, Tuple2<String, Row>> tuple = iterator.next();

                            Row row = tuple._2._2;
                            SessionDetail sessionDetail = new SessionDetail();
                            sessionDetail.setTaskid(taskid);
                            sessionDetail.setUserid(row.getLong(1));
                            sessionDetail.setSessionid(row.getString(2));
                            sessionDetail.setPageid(row.getLong(3));
                            sessionDetail.setActionTime(row.getString(4));
                            sessionDetail.setSearchKeyword(row.getString(5));
                            sessionDetail.setClickCategoryId(row.getLong(6));
                            sessionDetail.setClickProductId(row.getLong(7));
                            sessionDetail.setOrderCategoryIds(row.getString(8));
                            sessionDetail.setOrderProductIds(row.getString(9));
                            sessionDetail.setPayCategoryIds(row.getString(10));
                            sessionDetail.setPayProductIds(row.getString(11));

                            sessionDetails.add(sessionDetail);
                        }
                        ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAL();

                        sessionDetailDAO.insertBatch(sessionDetails);
                    }
                });

    }


    /**
     * 计算各session范围占比
     *
     * @param value
     * @param taskid
     */

    private static void calculateAndPersistAggrStat(String value, long taskid) {
        System.out.println(value);
        //从Accumulator统计串中获取值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.SESSION_COUNT));
        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));


        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double) visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double) visit_length_3m_10m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double) step_length_60 / (double) session_count, 2);

        //将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        //调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }


    /**
     * 获取Top10热门品类
     *
     * @param sessionid2detailRDD
     * @param taskid
     */
    private static List<Tuple2<CategorySortKey, String>> getTop10Category(
            JavaPairRDD<String, Row> sessionid2detailRDD,
            final long taskid) {
        /**
         * 第一步:获取符合条件的session访问过的所有品类
         */
        //获取所有session访问过的所有品类ID
        //访问过:指的是:点击过,下单过,支付过的品类<id,id>
        JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterable<Tuple2<Long, Long>> call(
                            Tuple2<String, Row> tuple)
                            throws Exception {
                        Row row = tuple._2;
                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

                        //点击ID
                        Long clickCategoryId = row.getLong(6);
                        if (clickCategoryId != null) {
                            list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
                        }

                        //订单品类ID
                        String orderCategoryIds = row.getString(8);
                        if (orderCategoryIds != null) {
                            String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                            for (String orderCategoryId : orderCategoryIdsSplited) {
                                list.add(new Tuple2<Long, Long>(
                                        Long.valueOf(orderCategoryId),
                                        Long.valueOf(orderCategoryId)));
                            }
                        }

                        //支付品类ID
                        String payCategoryIds = row.getString(10);
                        if (payCategoryIds != null) {
                            String[] payCategoryIdsSplited = payCategoryIds.split(",");
                            for (String payCategoryId : payCategoryIdsSplited) {
                                list.add(new Tuple2<Long, Long>(
                                        Long.valueOf(payCategoryId),
                                        Long.valueOf(payCategoryId)));
                            }
                        }

                        return list;
                    }
                });

        /**
         * 必须要进行去重
         * 如果不去重的话,会出现重复的categoryId,排序会对重复的categoryId已经countInfo进行排序
         * 最后很可能会拿到重复的数据
         */
        categoryidRDD = categoryidRDD.distinct();

        /**
         * 第二步:计算各品类的点击,下单,支付的次数
         */

        //访问明细中,其中三种范文行为是:点击,下单,支付
        //分别来计算个品类点击,下单和支付的次数,可以先对访问明细数据进行过滤
        //分别过滤点击,下单和支付行文,通过map,reduceByKey等算子进行计算

        //获得各个品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD =
                getClickCategoryId2CountRDD(sessionid2detailRDD);

        //获得各个品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryId2CountRdd =
                getOrderCategoryId2CountRdd(sessionid2detailRDD);

        //获得各个品类的支付次数
        JavaPairRDD<Long, Long> payCategoryId2CountRdd =
                getPayCategoryId2CountRdd(sessionid2detailRDD);

        /**
         * 第三步: join各品类与他的点击,下单,支付的次数
         *
         * categoryidRDD中,是包含了所有的符合条件的session,访问过的品类id
         *
         * 上面分别计算出来的三份,各品类的点击,下单和支付的次数,可能不是包含所有品类的
         * 比如,有的品类,就是只被点击过,但是没有人下单和支付
         *
         * 所以,这里就不能使用Join操作,而是使用leftOuterJoin操作,就是说,如果categoryidRDD不能
         * join到自己的某个数据,比如点击,或下单,或支付次数,那么该categoryidRDD还是要保留下来的
         * 只不过.没有join到的数据就是0了
         * <id,value>
         */
        JavaPairRDD<Long, String> categoryId2countRDD = joinCategoryAndDate(
                categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRdd,
                payCategoryId2CountRdd);

        /**
         * 第四步:自定义二次排序的Key
         */


        /**
         * 第五步: 将数据映射成<sortKey,info>的格式RDD,然后进行二次排序(降序)
         */
        JavaPairRDD<CategorySortKey, String> sortKey2countRDD = categoryId2countRDD.mapToPair(
                new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {
                    @Override
                    public Tuple2<CategorySortKey, String> call(
                            Tuple2<Long, String> tuple)
                            throws Exception {
                        String countInfo = tuple._2;
                        Long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                                countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
                        Long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                                countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
                        Long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                                countInfo, "\\|", Constants.FIELD_PAY_COUNT));

                        CategorySortKey categorySortKey = new CategorySortKey(clickCount, orderCount, payCount);
                        return new Tuple2<CategorySortKey, String>(categorySortKey, countInfo);
                    }
                });

        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD =
                sortKey2countRDD.sortByKey(false);

        /**
         * 第六步,用take(10)取出热门品类,并写入Mysql
         */
        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();

        List<Tuple2<CategorySortKey, String>> top10CategoryList =
                sortedCategoryCountRDD.take(10);

        for (Tuple2<CategorySortKey, String> tuple : top10CategoryList) {
            String countInfo = tuple._2;
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            Long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            Long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            Long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_PAY_COUNT));

            Top10Category top10Category = new Top10Category();
            top10Category.setTaskid(taskid);
            top10Category.setCategoryid(categoryid);
            top10Category.setClickCount(clickCount);
            top10Category.setOrderCount(orderCount);
            top10Category.setPayCount(payCount);

            top10CategoryDAO.insert(top10Category);
        }

        return top10CategoryList;
    }

    /**
     * 获取各品类点击次数RDD
     *
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(
            JavaPairRDD<String, Row> sessionid2detailRDD) {
        //计算各个品类的点击次数
        /**
         * 说明一下:
         * 这儿,是对完整的数据进行了filter过滤,过滤出来点击行为的数据
         * 点击行为的数据其实只是占总书记的一小部分
         * 所以过滤以后的RDD,每个partition的数据量,很有可能和我们之前说的一样,会很不均匀
         * 而且数据量肯定会变少变多
         *
         * 所以针对这种情况,还是比较适合用一下coalesce
         */
        JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return row.get(6) != null ? true : false;
                    }
                });
        //.coalesce(100)
        /**
         * 因为我们用的是local模式,并不需要设置分区和并行度数量
         * 因为性能很高,我们就不用再去设置
         */

        //映射
        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(
                new PairFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Tuple2<Long, Long> call(
                            Tuple2<String, Row> tuple) throws Exception {
                        long clickCategoryId = tuple._2.getLong(6);
                        return new Tuple2<Long, Long>(clickCategoryId, 1L);
                    }
                });

        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });
        return clickCategoryId2CountRDD;
    }

    /**
     * 获取各品类下单次数RDD
     *
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRdd(
            JavaPairRDD<String, Row> sessionid2detailRDD) {
        //首先判断id是否为空
        JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return row.getString(8) != null ? true : false;
                    }
                });
        JavaPairRDD<Long, Long> orderCategoryIdsRDD = orderActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterable<Tuple2<Long, Long>> call(
                            Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        String orderCategoryIds = row.getString(8);
                        String[] orderCategoryIdsSplited = orderCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

                        for (String orderCategoryId : orderCategoryIdsSplited) {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
                        }
                        return list;
                    }
                });

        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdsRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });
        return orderCategoryId2CountRDD;
    }

    /**
     * 获取各品类支付次数RDD
     *
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getPayCategoryId2CountRdd(
            JavaPairRDD<String, Row> sessionid2detailRDD) {
        //首先判断id是否为空
        JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return row.getString(10) != null ? true : false;
                    }
                });

        //格式为<id,count>
        JavaPairRDD<Long, Long> payCategoryIdsRDD = payActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterable<Tuple2<Long, Long>> call(
                            Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        String payCategoryIds = row.getString(10);
                        String[] payCategoryIdsSplited = payCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

                        for (String payCategoryId : payCategoryIdsSplited) {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));
                        }
                        return list;
                    }
                });

        //格式为<id,TotalCount>
        JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdsRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });

        return payCategoryId2CountRDD;
    }

    private static JavaPairRDD<Long, String> joinCategoryAndDate(
            JavaPairRDD<Long, Long> categoryidRDD,
            //<id,count>
            JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
            JavaPairRDD<Long, Long> orderCategoryId2CountRdd,
            JavaPairRDD<Long, Long> payCategoryId2CountRdd) {
        //如果用leftOuterJoin,就可能出现,右边那个RDD中,join过来时,没有值
        //所以Tuple中的第二个值用Optional<Long>类型,就代表,可能有值,可能也没有值
        JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD =
                categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD);

        JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(
                            Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple)
                            throws Exception {
                        Long categoryId = tuple._1;
                        Optional<Long> optional = tuple._2._2;
                        Long clickCount = 0L;
                        //如果有值
                        if (optional.isPresent()) {
                            clickCount = optional.get();
                        }

                        String value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" +
                                Constants.FIELD_CLICK_COUNT + "=" + clickCount;

                        return new Tuple2<Long, String>(categoryId, value);
                    }
                });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRdd)
                .mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(
                            Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
                            throws Exception {
                        Long categoryId = tuple._1;
                        Optional<Long> optional = tuple._2._2;
                        Long orderCount = 0L;
                        if (optional.isPresent()) {
                            orderCount = optional.get();
                        }

                        String value = tuple._2._1;
                        value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;

                        return new Tuple2<Long, String>(categoryId, value);
                    }
                });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRdd)
                .mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(
                            Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
                            throws Exception {
                        Long categoryId = tuple._1;
                        Optional<Long> optional = tuple._2._2;
                        Long payCount = 0L;
                        if (optional.isPresent()) {
                            payCount = optional.get();
                            System.out.println("********************************" + payCount);
                        }

                        String value = tuple._2._1;
                        value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;

                        return new Tuple2<Long, String>(categoryId, value);
                    }
                });


        return tmpMapRDD;
    }


    /**
     * 获取top10活跃session
     *
     * @param sc
     * @param taskid
     * @param sessionid2detailRDD
     * @param top10CategoryList
     */
    private static void getTop10Session(
            JavaSparkContext sc,
            final long taskid,
            JavaPairRDD<String, Row> sessionid2detailRDD,
            List<Tuple2<CategorySortKey, String>> top10CategoryList) {
        /**
         * 第一步:将top10热门品类的Id,生成一份RDD
         */
        List<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<Tuple2<Long, Long>>();

        for (Tuple2<CategorySortKey, String> category : top10CategoryList) {
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
                    category._2, "\\|", Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<Long, Long>(categoryid, categoryid));
        }

        //把一个数组变成一个RDD<categoryId,categoryId>
        JavaPairRDD<Long, Long> top10CategoryIdRDD =
                sc.parallelizePairs(top10CategoryIdList);

        /**
         * 第二步:计算top10品类被各session点击的次数
         */

        JavaPairRDD<String, Iterable<Row>> sessionid2detailsRDD = sessionid2detailRDD.groupByKey();

        //格式为<categoryId,value(sessionid,count)>
        JavaPairRDD<Long, String> categoryid2sessionCountRdd = sessionid2detailsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    @Override
                    public Iterable<Tuple2<Long, String>> call(
                            Tuple2<String, Iterable<Row>> tuple)
                            throws Exception {
                        String sessionid = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();
                        //计算出该Session,对每个品类的点击次数
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            if (row.get(6) != null) {
                                long categoryId = row.getLong(6);

                                Long count = categoryCountMap.get(categoryId);
                                if (count == null) {
                                    count = 0L;
                                }

                                count++;

                                categoryCountMap.put(categoryId, count);
                            }
                        }

                        //返回结果,<categoryId,sessionid,count>格式
                        List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();
                        for (Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()) {
                            Long categoryId = categoryCountEntry.getKey();
                            Long count = categoryCountEntry.getValue();
                            String value = sessionid + "," + count;
                            list.add(new Tuple2<Long, String>(categoryId, value));
                        }


                        return list;
                    }
                });

        //获取top10热门品类,被各个session点击的次数 <categoryId,value<sessionid,count>>
        JavaPairRDD<Long, String> top10CategorySessionCountRDD =
                top10CategoryIdRDD.join(categoryid2sessionCountRdd).mapToPair(
                        new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {
                            @Override
                            public Tuple2<Long, String> call(
                                    Tuple2<Long, Tuple2<Long, String>> tuple)
                                    throws Exception {
                                return new Tuple2<Long, String>(tuple._1, tuple._2._2);
                            }
                        });

        /**
         * 第三步:分组取TopN算法实现,获取每个品类的top10活跃用户
         */
        JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD =
                top10CategorySessionCountRDD.groupByKey();
        JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(
                            Tuple2<Long, Iterable<String>> tuple)
                            throws Exception {
                        Long categoryId = tuple._1;
                        Iterator<String> iterator = tuple._2.iterator();

                        //定义取top10的排序数组
                        String[] top10Sessions = new String[10];

                        while (iterator.hasNext()) {
                            String sessionCount = iterator.next();
                            Long count = Long.valueOf(sessionCount.split(",")[1]);

                            //遍历排序数组
                            for (int i = 0; i < top10Sessions.length; i++) {
                                //如果当前i位,没有数据,那么直接将i为数据则赋值为当前sessionCount
                                if (top10Sessions[i] == null) {
                                    top10Sessions[i] = sessionCount;
                                    break;
                                } else {
                                    //获取当前i位的sessionCount的count
                                    long _count = Long.valueOf(top10Sessions[i].split(",")[1]);
                                    //如果当前sessionCount比之前i的sessionCount要大
                                    if (count > _count) {
                                        //从排序数组的最后一位开始,到i位,所有数据往后挪一位
                                        for (int j = 9; j > i; j--) {
                                            top10Sessions[j] = top10Sessions[j - 1];
                                        }
                                        //将i位赋值为sessionCount
                                        top10Sessions[i] = sessionCount;
                                        break;
                                    }

                                    //比较小,继续for循环
                                }
                            }
                        }

                        //将数据写入Mysql表中
                        List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();

                        for (String sessionCount : top10Sessions) {
                            if (sessionCount != null) {
                                String sessonid = sessionCount.split(",")[0];
                                long count = Long.valueOf(sessionCount.split(",")[1]);

                                //将top10session插入MYSQL表中
                                Top10Session top10Session = new Top10Session();
                                top10Session.setTaskid(taskid);
                                top10Session.setCategoryid(categoryId);
                                top10Session.setSessionid(sessonid);
                                top10Session.setClickCount(count);

                                DAOFactory.getTop10SessionDAO().insert(top10Session);

                                //放入List
                                list.add(new Tuple2<String, String>(sessonid, sessonid));
                            }
                        }


                        return list;
                    }
                });

        /**
         * 第四步: 获取top10活跃session的明细数据,并写入MYSQL;
         */
        JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD =
                top10SessionRDD.join(sessionid2detailRDD);

/*        sessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            @Override
            public void call(
                    Tuple2<String, Tuple2<String, Row>> tuple)
                    throws Exception {
                Row row = tuple._2._2;
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskid(taskid);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));

                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAL();

                sessionDetailDAO.insert(sessionDetail);

            }
        });*/

        sessionDetailRDD.foreachPartition(
                new VoidFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>>() {
                    @Override
                    public void call(
                            Iterator<Tuple2<String, Tuple2<String, Row>>> iterator)
                            throws Exception {
                        List<SessionDetail> sessionDetails = new ArrayList<SessionDetail>();

                        while (iterator.hasNext()) {
                            Tuple2<String, Tuple2<String, Row>> tuple = iterator.next();

                            Row row = tuple._2._2;
                            SessionDetail sessionDetail = new SessionDetail();
                            sessionDetail.setTaskid(taskid);
                            sessionDetail.setUserid(row.getLong(1));
                            sessionDetail.setSessionid(row.getString(2));
                            sessionDetail.setPageid(row.getLong(3));
                            sessionDetail.setActionTime(row.getString(4));
                            sessionDetail.setSearchKeyword(row.getString(5));
                            sessionDetail.setClickCategoryId(row.getLong(6));
                            sessionDetail.setClickProductId(row.getLong(7));
                            sessionDetail.setOrderCategoryIds(row.getString(8));
                            sessionDetail.setOrderProductIds(row.getString(9));
                            sessionDetail.setPayCategoryIds(row.getString(10));
                            sessionDetail.setPayProductIds(row.getString(11));

                            sessionDetails.add(sessionDetail);
                        }
                        ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAL();

                        sessionDetailDAO.insertBatch(sessionDetails);
                    }
                });
    }

}
