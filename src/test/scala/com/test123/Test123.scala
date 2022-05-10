package com.test123

import com.keks.plan.builder.PlanUmlDiagramBuilder
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import utils.TestBase

import java.io.File
import scala.reflect.io.Directory
import com.keks.plan.implicits._
import com.keks.plan.parser.{DefaultExpressionParser, SparkLogicalRelationParser}
import com.keks.plan.write.UmlPlanSaver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, With}
import org.apache.spark.sql.catalyst.trees.Origin

class Test123 extends TestBase with HiveBase {

  val FINANCE_GL_COMMISSION_RATE_HIVE_TB = "finance_gl_commission_rate_hive_tb"
  val UW_PC_POLICY_PERIOD_HIVE_TB = "uw_pc_policy_period_hive_tb"
  val UW_PC_POLICY_PERIOD_HIVE_TB12 = "uw_pc_policy_period_hive_tb12"
  val FINANCE_GL_PC_DETAIL_HIVE_TB = "finance_gl_pc_detail_hive_tb"
  val FINANCE_GL_PC_DETAIL_HIVE_TB1 = "finance_gl_pc_detail_hive_tb1"
  val FINANCE_GL_PC_DETAIL_HIVE_TB2 = "finance_gl_pc_detail_hive_tb2"
  val UW_PC_EFF_DATED_FIELDS_HIVE_TB = "uw_pc_eff_dated_fields_hive_tb"
  val UW_PC_TRANSACTION_HIVE_TB = "uw_pc_transaction_hive_tb"
  val UW_PC_ALL_COST_HIVE_TB = "uw_pc_all_cost_hive_tb"
  val RULE_ID = "r001020015-1"

  "Test123" should "build report" in {
    createTable(FINANCE_GL_COMMISSION_RATE_HIVE_TB)
    createTable(UW_PC_POLICY_PERIOD_HIVE_TB)
    createTable(UW_PC_POLICY_PERIOD_HIVE_TB12)
    createTable(FINANCE_GL_PC_DETAIL_HIVE_TB)
    createTable(FINANCE_GL_PC_DETAIL_HIVE_TB1)
    createTable(FINANCE_GL_PC_DETAIL_HIVE_TB2)
    createTable(UW_PC_EFF_DATED_FIELDS_HIVE_TB)
    createTable(UW_PC_TRANSACTION_HIVE_TB)
    createTable(UW_PC_ALL_COST_HIVE_TB)

    val sql = """
      |with brokers as (
      |  select distinct
      |    b.policy_number,
      |    b.term_number,
      |    b.prod_cos_code,
      |    b.processed_date,
      |    b.rank_broker,
      |    eff_t.policy_period_public_id,
      |    eff_t.effective_date,
      |    eff_t.expiration_date
      |  from (
      |    select
      |    uw_pc.policy_number,
      |    uw_pc.term_number,
      |    uw_pc.prod_cos_code,
      |    uw_pc.processed_date,
      |    rank() over (partition by uw_pc.term_number, uw_pc.policy_number order by uw_pc.processed_date) as rank_broker
      |    from uw_pc_policy_period_hive_tb as uw_pc
      |    join (
      |      Select distinct policy_number, term_number
      |      from finance_gl_pc_detail_hive_tb
      |      where to_date(processed_date) = '2000-01-01') as finance_t
      |    on uw_pc.policy_number = finance_t.policy_number
      |      and uw_pc.term_number = finance_t.term_number
      |  ) b
      |  join uw_pc_eff_dated_fields_hive_tb as eff_t
      |     on b.prod_cos_code = eff_t.producer_code
      |  where rank_broker > 1
      |)
      |select amt1, amt2
      |from (
      |  select sum(case when f_t.accounting_entry_type = 'CR' then gl_amount_amt
      |                  when f_t.accounting_entry_type = 'DR' then - gl_amount_amt
      |                  end) as amt1
      |  from finance_gl_pc_detail_hive_tb as f_t
      |  join brokers on f_t.producer_code_of_record_code = brokers.prod_cos_code
      |    and f_t.policy_number = brokers.policy_number
      |    and f_t.term_number = brokers.term_number
      |  where f_t.gl_account_number = '4115'
      |    and f_t.job_source= 'WRITTEN PREMIUMS'
      |    and f_t.accounting_date >= brokers.effective_date
      |) ff
      |CROSS JOIN (
      |  select sum(tr.amount_amt) as amt2
      |  from uw_pc_transaction_hive_tb tr
      |  inner join uw_pc_policy_period_hive_tb pp
      |    on tr.policy_period_public_id = pp.public_id
      |  join brokers
      |    on pp.policy_number = brokers.policy_number
      |    and pp.term_number = brokers.term_number
      |  inner join uw_pc_all_cost_hive_tb cost
      |    on cost.public_id = tr.all_cost_public_id
      |    and cost.product = tr.product
      |    and cost.charge_pattern = 'Premium'
      |  where pp.edit_effective_date >= brokers.effective_date
      |    and pp.model_date >= brokers.effective_date
      |) gg
      |""".stripMargin

    val sql1 = """
                        with brokers as (
                          select distinct
                            b.policy_number,
                            b.term_number,
                            b.prod_cos_code,
                            b.processed_date,
                            b.rank_broker,
                            eff_t.policy_period_public_id,
                            eff_t.effective_date,
                            eff_t.expiration_date
                          from (
                            select
                            uw_pc.policy_number,
                            uw_pc.term_number,
                            uw_pc.prod_cos_code,
                            uw_pc.processed_date,
                            rank() over (partition by uw_pc.term_number, uw_pc.policy_number order by uw_pc.processed_date) as rank_broker
                            from uw_pc_policy_period_hive_tb as uw_pc
                            join (
                              Select distinct policy_number, term_number
                              from finance_gl_pc_detail_hive_tb
                              where to_date(processed_date) = '2021-07-22') as finance_t
                            on uw_pc.policy_number = finance_t.policy_number
                              and uw_pc.term_number = finance_t.term_number
                          ) b
                          join uw_pc_eff_dated_fields_hive_tb as eff_t
                             on b.prod_cos_code = eff_t.producer_code
                          where rank_broker > 1
                        )
                        select amt1, amt2
                        from (
                          select sum(case when f_t.accounting_entry_type = 'CR' then gl_amount_amt
                                          when f_t.accounting_entry_type = 'DR' then - gl_amount_amt
                                          end) as amt1
                          from finance_gl_pc_detail_hive_tb as f_t
                          join brokers on f_t.producer_code_of_record_code = brokers.prod_cos_code
                            and f_t.policy_number = brokers.policy_number
                            and f_t.term_number = brokers.term_number
                          where f_t.gl_account_number = '4115'
                            and f_t.job_source= 'WRITTEN PREMIUMS'
                            and f_t.accounting_date >= brokers.effective_date
                        ) ff
                        CROSS JOIN (
                          select sum(tr.amount_amt) as amt2
                          from uw_pc_transaction_hive_tb tr
                          inner join uw_pc_policy_period_hive_tb pp
                            on tr.policy_period_public_id = pp.public_id
                          join brokers
                            on pp.policy_number = brokers.policy_number
                            and pp.term_number = brokers.term_number
                          inner join uw_pc_all_cost_hive_tb cost on cost.public_id = tr.all_cost_public_id
                            and cost.product = tr.product
                            and cost.charge_pattern = 'Premium'
                          where pp.edit_effective_date >= brokers.effective_date
                            and pp.model_date >= brokers.effective_date
                        ) gg""".stripMargin

    val sql2 = """
      |with actv as (
      |    select distinct policy_number, term_number
      |    from uw_pc_policy_period_hive_tb12
      |    where cancelled_date is null
      |    -- and to_date(period_end) > $INPUT_DATE
      |)
      |,
      |unchanged as (
      |    select policy_number, term_number from (
      |        select
      |            pp.policy_number,
      |            pp.term_number,
      |            sum(pp.transaction_costrpt_amt) tr_sum
      |        from uw_pc_policy_period_hive_tb pp
      |        join actv
      |            on actv.policy_number = pp.policy_number and actv.term_number = pp.term_number
      |        -- where to_date(pp.processed_date) = $INPUT_DATE
      |        group by pp.policy_number, pp.term_number
      |        having sum(pp.transaction_costrpt_amt) = 0
      |        union
      |        select actv.policy_number, actv.term_number, count(pp.policy_number) tr_count
      |        from actv
      |        left join uw_pc_policy_period_hive_tb pp
      |            on actv.policy_number = pp.policy_number and actv.term_number = pp.term_number
      |            -- and to_date(processed_date) = $INPUT_DATE
      |        group by actv.policy_number, actv.term_number
      |        having count(pp.policy_number) = 0
      |    ) t1
      |)
      |
      |select cur.*, prev_sum
      |from (
      |    select
      |        dtl.policy_number,
      |        dtl.term_number,
      |        sum(case
      |            when accounting_entry_type = 'DR' then -gl_amount_amt
      |            when accounting_entry_type = 'CR' then gl_amount_amt
      |            end) cur_sum
      |    from finance_gl_pc_detail_hive_tb dtl
      |    inner join unchanged u
      |        on u.policy_number = dtl.policy_number and u.term_number = dtl.term_number
      |    where gl_account_number in ('4115', '4155')
      |        and job_source in ('bckdt_earnings','EARNED PREMIUMS' ,'rev_bckdt_earnings')
      |        --and to_date(processed_date) <= $INPUT_DATE
      |    group by dtl.policy_number, dtl.term_number
      |) cur
      |full join (
      |    select
      |        dtl.policy_number,
      |        dtl.term_number,
      |        sum(case
      |            when accounting_entry_type = 'DR' then -gl_amount_amt
      |            when accounting_entry_type = 'CR' then gl_amount_amt
      |            end) prev_sum
      |    from finance_gl_pc_detail_hive_tb dtl
      |    inner join unchanged u
      |        on u.policy_number = dtl.policy_number and u.term_number = dtl.term_number
      |    where gl_account_number in ('4115', '4155')
      |        and job_source in ('bckdt_earnings','EARNED PREMIUMS' ,'rev_bckdt_earnings')
      |        --and to_date(processed_date) <= date_sub($INPUT_DATE, 1)
      |    group by dtl.policy_number, dtl.term_number
      |) prev
      |on cur.policy_number = prev.policy_number and cur.term_number = prev.term_number
      |""".stripMargin

    val sql3 =
      """
        |select policy_number, term_number from (
        |        select
        |            pp.policy_number,
        |            pp.term_number
        |        from uw_pc_policy_period_hive_tb pp
        |    union
        |        select
        |            policy_number,
        |            term_number
        |        from uw_pc_policy_period_hive_tb12
        |    )
        |""".stripMargin

    val sql4 = """
                 |with actv as (
                 |    select distinct policy_number, term_number
                 |    from uw_pc_policy_period_hive_tb12
                 |    where cancelled_date is null
                 |)
                 |,
                 |unchanged as (
                 |    select policy_number, term_number from (
                 |        select
                 |            pp.policy_number,
                 |            pp.term_number
                 |        from uw_pc_policy_period_hive_tb pp
                 |    ) t1
                 |)
                 |
                 |select cur.*
                 |from (
                 |    select
                 |        dtl1.policy_number,
                 |        dtl1.term_number
                 |    from finance_gl_pc_detail_hive_tb1 dtl1
                 |    inner join unchanged u
                 |        on u.policy_number = dtl1.policy_number and u.term_number = dtl1.term_number
                 |) cur
                 |full join (
                 |    select
                 |        dtl2.policy_number,
                 |        dtl2.term_number
                 |    from finance_gl_pc_detail_hive_tb2 dtl2
                 |    inner join unchanged u
                 |        on u.policy_number = dtl2.policy_number and u.term_number = dtl2.term_number
                 |) prev
                 |on cur.policy_number = prev.policy_number and cur.term_number = prev.term_number
                 |""".stripMargin

    val df: DataFrame =  spark.sql(sql4)
    df.show(false)
    println(df.count())
    df.explain(true)
    val x = df.queryExecution.analyzed
    val logicNodes = parse(df.queryExecution.logical) ++
      parse(df.queryExecution.logical.asInstanceOf[With].cteRelations.head._2) ++
      parse(df.queryExecution.logical.asInstanceOf[With].cteRelations(1)._2)
    val analyzedNodes = parse(df.queryExecution.analyzed)
    val cachedNodes = parse(df.queryExecution.withCachedData)
    val optimizedNodes = parse(df.queryExecution.optimizedPlan)
    UnresolvedRelation
    // TODO add database to table names aka test_db.account
    // TODO check logic with cur.* Code detects it as column modifications
    df.printPlan(planParser = new SparkLogicalRelationParser(new DefaultExpressionParser()),
                 builder = new PlanUmlDiagramBuilder(),
                 entityName = s"r001020020_5",
                 reportDescription = "",
                 savePath = "examples",
                 saver = new UmlPlanSaver())
  }



  override def beforeEach(): Unit = {
    cleanup()
    spark.sql(s"create database $hiveSchema")
  }

  def createTable(tableName: String): Unit = {
    spark.read
      .options(csvOptions)
      .csv(s"$dataLocation/input/$tableName$dataFileSuffix")
      .write
      .saveAsTable(s"$hiveSchema.$tableName")
    spark.sql(s"REFRESH TABLE $hiveSchema.$tableName")
    spark.sql(s"USE $hiveSchema")
  }

  def parse(plan: LogicalPlan): Array[(Origin, LogicalPlan)] = {
    val x: Array[(Origin, LogicalPlan)] = Array((plan.origin, plan))
    val y = plan.children.flatMap(parse).toArray
    x ++ y
  }


}

trait HiveBase extends BeforeAndAfterEach with BeforeAndAfterAll {
  self: TestBase =>
  val hiveSchema = "test_db"
  val VALIDATION_RULE_TABLE = "validation_rule"
  val dataLocation = "src/test/resources/data"
  val dataFileSuffix = ".tsv"
  val csvOptions = Map("delimiter" -> "\t", "header" -> "true", "timestampFormat" -> "yyyy-MM-dd HH:mm:ss",
                       "nullValue" -> "NULL")

  override def afterEach(): Unit = cleanup()

  override def beforeEach(): Unit = {
    initWarehouse()
//    refresh()
  }

  def cleanup(): Unit = {
    spark.sql(s"drop database if exists $hiveSchema cascade")
    new Directory(new File("spark-warehouse")).deleteRecursively()
  }

//  private def refresh(): Unit =
//    dataTables.foreach(table => spark.sql(s"REFRESH TABLE $hiveSchema.$table"))

  private def initWarehouse(): Unit = {

    cleanup()
    spark.sql(s"create database $hiveSchema")

//    dataTables
//      .foreach(table => {
//        spark.sql(s"drop table if exists $hiveSchema.$table")
//        val writer = spark.read
//          .options(csvOptions)
//          .csv(s"$dataLocation/input/$table$dataFileSuffix")
//          .write
//
//        (if (table == VALIDATION_RULE_TABLE) writer.partitionBy("id") else writer)
//          .saveAsTable(s"$hiveSchema.$table")
//      })
  }

//  private val dataTables =
//    new File(s"$dataLocation/input").listFiles
//      .filter(_.getName.endsWith(dataFileSuffix))
//      .map(f => f.getName.stripSuffix(dataFileSuffix))
}

