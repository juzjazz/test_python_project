# coding=utf-8

import datetime
import time
from dbutils.TFlowDbObject import TFlowDbObject
from common import Constants



class sp_error_noInfo_statistics:

    def __init__(self):
        self.start_date = ''
        self.v_object_name = 'SP_ERROR_NOINFO_STATISTICS'
        self.v_info = None
        self.i_date = '${i_data}'
        self.v_reson = ''
        self.v_reson_remark = ''
        self.o_see_flag = 0
        self.o_end_data = 0
        self.o_data_user_id = 0
        self.n_statue = 0
        self.v_remark = ''
        self.err_db = None
        self.err_conn = None
        self.err_cur = None
        self.account_db = None
        self.account_conn = None
        self.account_cur = None
        self.account2_db = None
        self.account2_conn = None
        self.account2_cur = None
        self.account3_db = None
        self.account3_conn = None
        self.account3_cur = None
        self.account4_db = None
        self.account4_conn = None
        self.account4_cur = None
        self.sps_db = None
        self.sps_conn = None
        self.sps_cur = None
        self.sps_db2 = None
        self.sps_conn2 = None
        self.sps_cur2 = None
        self.sps_db3 = None
        self.sps_conn3 = None
        self.sps_cur3 = None
        self.sps_db4 = None
        self.sps_conn4 = None
        self.sps_cur4 = None
        self.pub_db = None
        self.pub_conn = None
        self.pub_cur = None
        self.param_db = None
        self.param_cur = None
        self.param_conn = None
        self.city_ref = {'23': '0514', '20': '0513', '21': '0523', '22': '0515', '19': '0510', '12': '0517',
                         '18': '0511'
            , '17': '0519', '14': '025', '13': '0527', '15': '0518', '16': '0516', '11': '0512'}

    def _init_db(self):
        try:
            print(f"初始化数据库")
            self.err_db = TFlowDbObject(Constants.NONE_CITY_ID, "SUB_SYS_ERROR")
            self.err_conn = self.err_db.connectDB()
            self.err_cur = self.err_conn.cursor()
            self.account_db = TFlowDbObject(Constants.YZ_CITY_ID, "SUB_SYS_ACCOUNTING")
            self.account_conn = self.account_db.connectDB()
            self.account_cur = self.account_conn.cursor()
            self.account2_db = TFlowDbObject(Constants.ZJ_CITY_ID, "SUB_SYS_ACCOUNTING")
            self.account2_conn = self.account2_db.connectDB()
            self.account2_cur = self.account2_conn.cursor()
            self.account3_db = TFlowDbObject(Constants.NJ_CITY_ID, "SUB_SYS_ACCOUNTING")
            self.account3_conn = self.account3_db.connectDB()
            self.account3_cur = self.account3_conn.cursor()
            self.account4_db = TFlowDbObject(Constants.SZ_CITY_ID, "SUB_SYS_ACCOUNTING")
            self.account4_conn = self.account4_db.connectDB()
            self.account4_cur = self.account4_conn.cursor()
            self.sps_db = TFlowDbObject(Constants.YZ_CITY_ID, "SUB_SYS_ACCOUNTING_SPS")
            self.sps_conn = self.sps_db.connectDB()
            self.sps_cur = self.sps_conn.cursor()
            self.sps_db2 = TFlowDbObject(Constants.HA_CITY_ID, "SUB_SYS_ACCOUNTING_SPS")
            self.sps_conn2 = self.sps_db2.connectDB()
            self.sps_cur2 = self.sps_conn2.cursor()
            self.sps_db3 = TFlowDbObject(Constants.SQ_CITY_ID, "SUB_SYS_ACCOUNTING_SPS")
            self.sps_conn3 = self.sps_db3.connectDB()
            self.sps_cur3 = self.sps_conn3.cursor()
            self.sps_db4 = TFlowDbObject(Constants.SZ_CITY_ID, "SUB_SYS_ACCOUNTING_SPS")
            self.sps_conn4 = self.sps_db4.connectDB()
            self.sps_cur4 = self.sps_conn4.cursor()
            self.pub_db = TFlowDbObject(Constants.NONE_CITY_ID, Constants._SUB_SYS_BOSSPUB)
            self.pub_conn = self.pub_db.connectDB()
            self.pub_cur = self.pub_conn.cursor()
            self.param_db = TFlowDbObject(Constants.NONE_CITY_ID, "SUB_SYS_PARAM")
            self.param_conn = self.param_db.connectDB()
            self.param_cur = self.param_conn.cursor()
            print("初始化数据库完成")
        except Exception as e:
            print(f"初始化数据库异常_init_pub_db, 异常信息：[{e}]")
            raise e

    def _cur_msisdn_error719(self):
        print("进去719查询话单")
        sql = "select /* QUERY_TIMEOUT(14400000000)*/  CDR_DATE,ERROR_CODE,SOURCE_TYPE,MSISDN,AREA_CODE,CDR_CNT,CDR_DURATION,INURE_DATA_CNT," \
              "REMARK,CDR_MIN_START_TIME,CDR_MAX_END_TIME,REASON,END_DATE,IMSI,REASON1,USER_ID " \
              ",a.rowid from ERR_NOINFO_STATISTICS a where a.cdr_date = '%s' and a.error_code " \
              "in( '719','E2408')" % self.i_date
        print(f"719里面sql{sql} ")
        self.err_cur.execute(sql)
        data = self.err_cur.fetchall()
        return data

    def _cur_msisdn_error715(self):
        sql = "select /* QUERY_TIMEOUT(14400000000)*/  CDR_DATE,ERROR_CODE,SOURCE_TYPE,MSISDN,AREA_CODE,CDR_CNT,CDR_DURATION,INURE_DATA_CNT," \
              "REMARK,CDR_MIN_START_TIME,CDR_MAX_END_TIME,REASON,END_DATE,IMSI,REASON1,USER_ID " \
              ",a.rowid from ERR_NOINFO_STATISTICS a where a.cdr_date = '%s' and a.error_code = '715'" % self.i_date
        print(f"715 {sql}")
        self.err_cur.execute(sql)
        data = self.err_cur.fetchall()
        print("查询715jieshu")
        return data

    def _p_check_personal_data(self, i_msisdn, iarea_code, i_cdr_begin_time, i_cdr_end_time):
        v_dblink = None
        v_sql_resource = None
        v_sql_users = None
        v_sql_brand = None
        v_service_code = ''
        v_prod_id = None
        v_area_code = None
        n_total_cnt = None
        n_cdr_inure_cnt = None
        n_uninure_cnt = None
        n_xiaohu = None
        n_users_total_cnt = None
        n_users_cnt = None
        n_user_id = 0
        n_city_id = None
        n_prod_inure_flag = None
        n_prod_instance_id = None
        ld_end_date = 0
        ld_res_end_date = None
        try:
            if i_cdr_end_time is not None:
                i_cdr_end_time = self._date_formart(str(i_cdr_end_time))
            if i_cdr_begin_time is not None:
                i_cdr_begin_time = self._date_formart(str(i_cdr_begin_time))
            v_link_sql = "select t.db_link from err_cc_city_info t where t.city_code = trim(:iarea_code)"
            param = {'iarea_code': iarea_code}
            self.err_db.execute(v_link_sql, param, self.err_cur)
            v_link_data = self.err_cur.fetchone()
            if v_link_data is None:
                v_dblink = '1'
            else:
                v_dblink = v_link_data[0]
            if str(v_dblink).endswith("1"):
                tmp_cur = self.account_cur
                tmp_conn = self.account_conn
                tmp_db = self.account_db
            elif str(v_dblink).endswith("2"):
                tmp_cur = self.account2_cur
                tmp_conn = self.account2_conn
                tmp_db = self.account2_db
            elif str(v_dblink).endswith("3"):
                tmp_cur = self.account3_cur
                tmp_conn = self.account3_conn
                tmp_db = self.account3_db
            elif str(v_dblink).endswith("4"):
                tmp_cur = self.account4_cur
                tmp_conn = self.account4_conn
                tmp_db = self.account4_db
            sql = 'select begin_msisdn from exd_csp_type_segment where csp_type=4'
            self.param_cur.execute(sql)
            csp_data = self.param_cur.fetchall()
            if csp_data is not None:
                for data in csp_data:
                    if i_msisdn is not None and str(i_msisdn).startswith(data[0]):
                        self.v_reson = str(data[0]) + '虚拟运营商'
                        return
            # if str(i_msisdn).startswith('170'):
            #     self.v_reson = '170虚拟运营商'
            #     return
            # or str(i_msisdn).startswith('172')
            # if str(i_msisdn).startswith('1064'):
            if i_cdr_end_time is None and str(i_cdr_end_time) == '0':
                i_cdr_end_time = '20991231245959'
            if iarea_code is None:
                iarea_code = '0'
            if str(i_msisdn) is not None:
                city_id_sqls = 'select area_code from exd_domestic_msisdn t1 where  :i_msisdn ' \
                               '  between begin_msisdn and end_msisdn and :i_start_time > begin_date and ' \
                               ':i_end_time< end_date '
                param = {'i_start_time': str(i_cdr_begin_time),
                         'i_msisdn': i_msisdn,
                         'i_end_time': str(i_cdr_end_time)}
                self.param_db.execute(city_id_sqls, param, self.param_cur)
                area_data = self.param_cur.fetchone()
                if area_data is None:
                    city_sql = " select city_id from REC_SVCNUMBER_TRANS_CITY where service_number=:i_msisdn " \
                               "and to_date(:i_start_date,'yyyyMMddHH24miss') > start_date and" \
                               " to_date(:i_end_date, 'yyyyMMddHH24miss') < end_date"
                    param = {'i_msisdn': i_msisdn,
                             'i_start_date': str(i_cdr_begin_time),
                             'i_end_date': str(i_cdr_end_time)}
                    self.account2_db.execute(city_sql, param, self.account2_cur)
                    city_data = self.account2_cur.fetchone()
                    if city_data is not None:
                        area = self.city_ref.get(str(city_data[0]))
                        if area is not None:
                            if str(area) != str(iarea_code):
                                self.v_reson = '局数据异常 号码归属地与局数据号码归属不一致'
                                self.v_reson_remark = '资料归属地' + str(area) + '局数据归属地' + str(iarea_code)
                                return
                        else:
                            self.v_reson = '局数据异常 号码归属地与局数据号码归属不一致'
                            self.v_reson_remark = '需要手工核实'
                            return
                else:
                    area_code_data = area_data
                    # city_id_sql = "select t.CITY_ID  from users t where t.SERVICE_NUMBER=:i_msisdn and t.STATUS in " \
                    #               "(10,28,30) and rownum<2"
                    # if tmp_cur is None:
                    #     tmp_cur = self.account_cur
                    # param = {'i_msisdn': i_msisdn}
                    # tmp_db.execute(city_id_sql, param, tmp_cur)
                    # n_city_id = tmp_cur.fetchone()
                    # if n_city_id is None:
                    #     n_city_id_num = 0
                    #     # self.v_reson = '局数据异常 号码归属地与局数据号码归属不一致'
                    #     # self.v_reson_remark = '资料归属地为空 局数据归属地' + str(iarea_code)
                    #     # return
                    # else:
                    #     n_city_id_num = int(n_city_id[0])
                    # area_code_sql = "select t1.city_code  from err_cc_city_info t1 where t1.city_id=:n_city_id"
                    # param = {'n_city_id': n_city_id_num}
                    # self.err_db.execute(area_code_sql, param, self.err_cur)
                    # area_code_data = self.err_cur.fetchone()
                    if area_code_data is None:
                        area_code_data_num = 0
                        # self.v_reson = '局数据异常 号码归属地与局数据号码归属不一致'
                        # self.v_reson_remark = '资料归属地' + str(area_code_data) + '局数据归属地' + str(iarea_code)
                        # return
                    else:
                        area_code_data_num = str(area_code_data[0])
                    if str(iarea_code) != str(area_code_data_num):
                        self.v_reson = '局数据异常 号码归属地与局数据号码归属不一致'
                        self.v_reson_remark = '资料归属地' + str(area_code_data_num) + '局数据归属地' + str(iarea_code)
                        return
            self.o_see_flag = 1
            # 校验资源表是否有数据
            v_sql_resource_sql = "select count(1) ,count(case when start_date < to_date(:i_cdr_begin_time," \
                                 " 'yyyymmddhh24miss') and nvl(end_date, to_date('20991231', 'yyyymmdd')) > " \
                                 "to_date(:i_cdr_end_time, 'yyyymmddhh24miss') then 1 end) " \
                                 ",count(case when start_date > to_date(:i_cdr_begin_time, 'yyyymmddhh24miss')" \
                                 " and nvl(end_date, to_date('20991231', 'yyyymmdd')) > start_date then 1 end)" \
                                 "  from user_resource  where resource_type = 1" \
                                 " and resource_value =:i_msisdn"
            param = {'i_cdr_begin_time': i_cdr_begin_time,
                     'i_cdr_end_time': i_cdr_end_time,
                     'i_msisdn': i_msisdn}
            tmp_db.execute(v_sql_resource_sql, param, tmp_cur)
            resource_data = tmp_cur.fetchone()
            n_total_cnt = resource_data[0]
            n_cdr_inure_cnt = resource_data[1]
            n_uninure_cnt = resource_data[2]
            v_sql_user = "select count(1) from users where service_number =  :i_msisdn "
            param = {'i_msisdn': str(i_msisdn).replace('None', '0')}
            tmp_db.execute(v_sql_user, param, tmp_cur)
            n_users_total_cnt = tmp_cur.fetchone()
            if n_users_total_cnt is None:
                n_users_total_cnt = [0]
            if n_total_cnt == 0 and n_users_total_cnt[0] == 0:
                self.v_reson = '系统中无资料'
            elif n_cdr_inure_cnt > 1:
                self.v_reson = '资料异常 资源表存在多条生效手机号记录'
            else:
                v_sql_users = "select count(case when status in (20, 22,23,24, 27) then 1 end),count(case when status " \
                              "in (10, 28 , 30, 31) then 1 end) from users where service_number = :i_msisdn"
                param = {'i_msisdn': i_msisdn}
                tmp_db.execute(v_sql_users, param, tmp_cur)
                v_sql_users_data = tmp_cur.fetchone()
                n_xiaohu = v_sql_users_data[0]
                n_users_cnt = v_sql_users_data[1]
                if n_cdr_inure_cnt == 0:
                    if n_xiaohu > 0 and n_uninure_cnt == 0:
                        self.v_reson = '系统中已销户'
                        if str(i_msisdn).startswith('106'):
                            self.v_reson = '物联网销户'
                        v_sql_users = "select min(terminal_date) keep(dense_rank first order by terminal_date desc" \
                                      " nulls last), min(user_id) keep(dense_rank first order by terminal_date desc nulls" \
                                      " last) from users where service_number =  :i_msisdn  and status in (20, 24, 27) and " \
                                      "terminal_date is not null"
                        param = {'i_msisdn': i_msisdn}
                        tmp_db.execute(v_sql_users, param, tmp_cur)
                        tmp_data1 = tmp_cur.fetchone()
                        ld_end_date = tmp_data1[0]
                        if ld_end_date is None:
                            ld_end_date = 0
                        n_user_id = tmp_data1[1]
                    else:
                        if n_uninure_cnt > 0:
                            self.v_reson = '话单时间早于资料生效时间'
                        else:
                            self.v_reson = '资料异常 资源表无有效手机号记录'
                        v_sql_resource_sql = "select min(end_date) keep(dense_rank first order by end_date desc nulls " \
                                             "first),min(service_code) keep(dense_rank first order by end_date desc " \
                                             "nulls first),min(user_id) keep(dense_rank first order by end_date desc " \
                                             "nulls first),min(prod_instance_id) keep(dense_rank first order by end_" \
                                             "date desc nulls first) from user_resource where " \
                                             "resource_type = 1 and resource_value =  :i_msisdn"
                        param = {'i_msisdn': i_msisdn}
                        tmp_db.execute(v_sql_resource_sql, param, tmp_cur)
                        resource_data = tmp_cur.fetchone()
                        ld_res_end_date = resource_data[0]
                        if isinstance(ld_res_end_date, datetime.datetime):
                            ld_res_end_date = datetime.datetime.strftime(ld_res_end_date, '%Y%m%d%H%M%S')
                        elif isinstance(ld_res_end_date, str):
                            ld_res_end_date = self._date_formart(ld_res_end_date)
                        v_service_code = resource_data[1]
                        n_user_id = resource_data[2]
                        n_prod_instance_id = resource_data[3]
                        if v_service_code and v_service_code is not None:
                            v_service_code = str(v_service_code)
                            v_sql = "select min(t.prodid)   from f_product_service_code t where " \
                                    "t.serviceid=:v_service_code"
                            param = {'v_service_code': v_service_code}
                            self.err_db.execute(v_sql, param, self.err_cur)
                            v_prod_id = self.err_cur.fetchone()
                            v_sql = "select max(nvl(end_date,to_date('29991231','yyyymmdd'))),sign(count(*))" \
                                    " from user_product where user_id = :n_user_id and product_id = :v_prod_id " \
                                    "and instance_id= nvl(:n_prod_instance_id,instance_id) and" \
                                    " start_date<nvl(to_date(:ld_res_end_date,'yyyyMMddHH24miss'),to_date('29991231','yyyymmdd'))"
                            try:
                                if v_prod_id is not None:
                                    if v_prod_id[0] is None:
                                        v_prod_id_data = 0
                                    else:
                                        v_prod_id_data = int(v_prod_id[0])
                                    if n_prod_instance_id is None:
                                        n_prod_instance_id = 0
                                    if n_user_id is None:
                                        n_user_id = 0
                                    param = {'n_user_id': n_user_id,
                                             'v_prod_id': v_prod_id_data,
                                             'n_prod_instance_id': n_prod_instance_id,
                                             'ld_res_end_date': str(ld_res_end_date).replace('\'None\'',
                                                                                             '20991231125959')}
                                    tmp_db.execute(v_sql, param, tmp_cur)
                                    v_sql_data_resource = tmp_cur.fetchone()
                            except Exception as e:
                                print(f"就是这条破Sql{v_sql},param{param}")
                                print(f"异常 {e}")
                                raise e
                            ld_end_date = v_sql_data_resource[0]
                            n_prod_inure_flag = v_sql_data_resource[1]
                            if ld_end_date is None:
                                # ld_end_date = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                                ld_end_date = 0
                            else:
                                if isinstance(ld_end_date, datetime.datetime):
                                    ld_end_date = ld_end_date.strftime("%Y%m%d%H%M%S")
                            if str(i_cdr_begin_time) > str(ld_end_date):
                                self.v_reson = '功能产品已关闭'
                            if n_prod_inure_flag == 0:
                                self.v_reson = '无相关产品或数据已清理掉'
                        else:
                            if n_xiaohu == 0 and n_users_cnt == 0:
                                self.v_reson = '系统中无资料'
                else:
                    v_sql_resource_sql = "select distinct first_value(user_id) over(order by start_date)," \
                                         "first_value(service_code) over(order by start_date) from user_resource " \
                                         "where resource_type = 1 and resource_value =  :i_msisdn and start_date < " \
                                         "to_date(:i_cdr_begin_time, 'yyyymmddhh24miss') and nvl(end_date," \
                                         " to_date('20991231', 'yyyymmdd')) > to_date(:i_cdr_end_time," \
                                         " 'yyyymmddhh24miss')"
                    param = {'i_msisdn': i_msisdn,
                             'i_cdr_begin_time': i_cdr_begin_time,
                             'i_cdr_end_time': i_cdr_end_time}
                    tmp_db.execute(v_sql_resource_sql, param, tmp_cur)
                    v_sql_resource_sql_data = tmp_cur.fetchone()
                    n_user_id = v_sql_resource_sql_data[0]
                    v_service_code = v_sql_resource_sql_data[1]
                    if n_users_cnt == 0 and v_service_code is None:
                        self.v_reson = '资料异常 用户表无数据'
                    else:
                        v_sql_brand = "select count(1) n_total_cnt, count(case when start_date < to_date(:i_cdr_begin_time , " \
                                      "'yyyymmddhh24miss') and nvl(end_date, to_date('20991231', 'yyyymmdd')) " \
                                      "> to_date(:i_cdr_end_time, 'yyyymmddhh24miss') then 1 end) n_cdr_inure_cnt, " \
                                      "count(case when start_date > to_date(:i_cdr_begin_time, 'yyyymmddhh24miss') " \
                                      "and nvl(end_date, to_date('20991231', 'yyyymmdd')) > start_date then " \
                                      "1 end) n_uninure_cnt from user_brand " \
                                      "where user_id = :n_user_id"
                        param = {'i_cdr_begin_time': i_cdr_begin_time,
                                 'i_cdr_end_time': i_cdr_end_time,
                                 'n_user_id': n_user_id}
                        tmp_db.execute(v_sql_brand, param, tmp_cur)
                        brand_data = tmp_cur.fetchone()
                        n_total_cnt = brand_data[0]
                        n_cdr_inure_cnt = brand_data[1]
                        n_uninure_cnt = brand_data[2]
                        if n_cdr_inure_cnt == 0 and n_uninure_cnt == 0:
                            self.v_reson = '资料异常 品牌表无有效数据'
                        elif n_cdr_inure_cnt == 0 and n_uninure_cnt > 0:
                            self.v_reson = '话单时间无品牌资料 资料未生效'
                        elif n_total_cnt > 100:
                            self.v_reson = '垃圾数据 品牌表资料超过100条'
                        else:
                            self.v_reson = '需手工核实资料'
                            self.o_see_flag = 0
            o_remark_sql = "select nvl(min(t.product_name), :v_service_code)  from f_product_service_code" \
                           " t where t.SERVICEID = :v_service_code"
            param = {'v_service_code': str(v_service_code).replace('None', '')}
            self.err_db.execute(o_remark_sql, param, self.err_cur)
            o_remark_data = self.err_cur.fetchone()
            if o_remark_data is None:
                self.v_remark = ""
            else:
                self.v_remark = str(o_remark_data[0]).replace('None', '')
            self.o_end_data = ld_end_date
            self.o_data_user_id = n_user_id
        except Exception as es:
            print(f"瓜皮异常{es}")
            raise es

    def _sp_analyze_terminal_user(self):
        print("进入方法 _sp_analyze_terminal_user")
        v_new_reason = None
        v_sub_reason = None
        str_reson = ''
        str_success = ''
        each_sql = "select CDR_DATE,ERROR_CODE,SOURCE_TYPE,MSISDN,AREA_CODE,CDR_CNT,CDR_DURATION,INURE_DATA_CNT," \
                   "REMARK,CDR_MIN_START_TIME,CDR_MAX_END_TIME,REASON,END_DATE,IMSI,REASON1,USER_ID" \
                   ",t.rowid from ERR_NOINFO_STATISTICS t where t.cdr_date = :i_date and" \
                   " t.error_code = '719' and t.end_date>trunc(add_months(sysdate,-1),'mm') and t.reason  " \
                   "in ('系统中已销户','功能产品已关闭') and t.source_type in ('A', 'Q', 'M', 'E') "
        param = {'i_date': int(self.i_date)}
        self.err_db.execute(each_sql, param, self.err_cur)
        data = self.err_cur.fetchall()
        print(f"sp analyz 查询 结束 大小{len(data)}")
        for v in data:
            try:
                code = v[4]
                if code is None or code == 'None':
                    code = '0'
                v_dblink_sql = "select t.db_link  from err_cc_city_info t where t.city_code = :area_code"
                param = {'area_code': code}
                self.err_db.execute(v_dblink_sql, param, self.err_cur)
                v_dblinks = self.err_cur.fetchone()
                if v_dblinks is None:
                    continue
                v_dblink = v_dblinks[0]
                if v_dblink is None:
                    v_dblink = 1
                tmp_cur = None
                tmp_conn = None
                tmp_acct = None
                tmp_db = None
                tmp_acct_db = None
                if str(v_dblink).rstrip().endswith("1"):
                    tmp_cur = self.sps_cur
                    tmp_acct = self.account_cur
                    tmp_db = self.sps_db
                    tmp_acct_db = self.account_db
                elif str(v_dblink).rstrip().endswith("2"):
                    tmp_cur = self.sps_cur2
                    tmp_conn = self.sps_conn2
                    tmp_acct = self.account2_cur
                    tmp_acct_db = self.account2_db
                    tmp_db = self.sps_db2
                elif str(v_dblink).rstrip().endswith("3"):
                    tmp_cur = self.sps_cur3
                    tmp_conn = self.sps_conn3
                    tmp_acct = self.account3_cur
                    tmp_acct_db = self.account3_db
                    tmp_db = self.sps_db3
                elif str(v_dblink).rstrip().endswith("4"):
                    tmp_cur = self.sps_cur4
                    tmp_conn = self.sps_conn4
                    tmp_acct = self.account4_cur
                    tmp_acct_db = self.account4_db
                    tmp_db = self.sps_db4
                v_sql = "select min(imsi) keep(dense_rank first order by start_date desc) from users where " \
                        "service_number=:service_number"
                if v[3] is None:
                    server_number = '0'
                else:
                    server_number = v[3]
                param = {'service_number': str(server_number)}
                print(f" server_number={server_number},v8={v[8]}, 连接{tmp_acct_db},{tmp_acct}")
                tmp_acct_db.execute(v_sql, param, tmp_acct)
                v_imsi_data = tmp_acct.fetchone()
                if v_imsi_data is None:
                    v_imsi = '0'
                else:
                    v_imsi = v_imsi_data[0]
                print(f"this problem missing v_imsi={v_imsi}")
                if str(v_imsi) != str(server_number) and v[8] is not None:
                    v_new_reason = str(v[11]) + '系统与网元IMSI不一致'
                    v_sub_reason = '系统IMSI' + str(v_imsi) + '与网元IMSI' + str(v[3]) + '不一致(需CRM重新发送工单)'
                else:
                    str_success = '工单发送成功 (1天内的话单)'
                    v_sql = "select  min(t2.service_code ||  '工单发送成功(需要网络部核实) 发送时间' || " \
                            "to_char(t2.finish_date, 'yyyymmddhh24miss')) keep(dense_rank first  order by " \
                            "t2.finish_date desc) remark from sps_cloud_order_his" \
                            " t1, sps_c_service_operation_his t2 where t1.oper_code " \
                            "=:server_number and t2.service_code in ('1901', '1925') and t1.order_id =" \
                            " t2.order_id and t1.order_request_sequence = t2.order_request_sequence and t2.status = " \
                            "2 and t2.finish_date>to_date(:end_date,'yyyyMMddHH24miss')"
                    end_date = v[12]
                    if end_date is None or end_date == 'NULL':
                        end_date = '20991231245959'
                    else:
                        end_date = self._date_formart(str(v[12]))
                    param = {'server_number': server_number,
                             'end_date': end_date}
                    tmp_db.execute(v_sql, param, tmp_cur)
                    v_sub_reason_data = tmp_cur.fetchone()
                    if v_sub_reason_data is None:
                        v_sub_reason = None
                    else:
                        v_sub_reason = v_sub_reason_data[0]
                    print(f"v_sub_version={v_sub_reason}")
                    if v_sub_reason is not None:
                        if int(v[10]) < int(self._date_formart(str(v[12]))):
                            v_new_reason = str(v[11]) + str(str_success)
                        else:
                            v_new_reason = str(v[11]) + '工单发送成功'
                    else:
                        v_sql = "select  count(*) from sps_order_his " \
                                "t1, sps_c_service_operation_his t2 where " \
                                "t1.oper_code =:server_number and t2.service_code in ('1901', '1925')" \
                                " and t1.order_id = t2.order_id and t1.order_request_sequence = " \
                                "t2.order_request_sequence and t2.finish_date>to_date(:end_date,'yyyyMMddHH24miss')"
                        end_date = v[12]
                        if end_date is None:
                            end_date = '20991231235959'
                        else:
                            end_date = self._date_formart(str(end_date))
                        param = {'server_number': str(server_number),
                                 'end_date': str(end_date)}
                        tmp_db.execute(v_sql, param, tmp_cur)
                        n_cnt1 = tmp_cur.fetchone()
                        if n_cnt1[0] == 0:
                            v_new_reason = str(v[14]).replace('None', '') + 'CRM未发送销户请求'
                            v_sub_reason = 'CRM未发送销户请求'
                        else:
                            v_sql = "select substr('工单发送失败 '||" \
                                    "listagg(operation_type_code||nc_channel_code||recv_date||send_date||err_code" \
                                    "||err_desc),1,2000) from (select '工单类型 '" \
                                    "||operation_type_code operation_type_code, ' 通道号 '||min(t.nc_channel_code)" \
                                    " keep(dense_rank last order by create_date desc) nc_channel_code, ' " \
                                    "返回时间'||to_char(min(t.recv_date) keep(dense_rank last order by create_date desc) " \
                                    ",'yyyymmddhh24miss')recv_date, ' 发送时间'||to_char(min(t.send_date) " \
                                    "keep(dense_rank last order by create_date desc) ,'yyyymmddhh24miss')send_date," \
                                    " ' 错误代码'||min(t.err_code) keep(dense_rank last order by create_date desc) " \
                                    "err_code, ' 错误描述'||min(err_desc) keep(dense_rank last order by " \
                                    "create_date desc) err_desc from sps_cloud_operation_fail " \
                                    "t where t.oper_code = :server_number and t.operation_type_code in ('10011901'," \
                                    " '10011925') and send_date > to_date(:end_date,'yyyyMMddHH24miss') group by operation_type_code)"
                            end_date = v[12]
                            if end_date is None:
                                end_date = '20991231235959'
                            else:
                                end_date = self._date_formart(str(end_date))
                            param = {'server_number': str(server_number),
                                     'end_date': str(end_date)}
                            tmp_db.execute(v_sql, param, tmp_cur)
                            v_sub_reason_data = tmp_cur.fetchone()
                            if v_sub_reason_data is None:
                                v_new_reason = ''
                            else:
                                v_new_reason = v_sub_reason_data[0]
            except Exception as es:
                tmp = "用户msisdn %s销户分析异常rowid %s" % (v[3], v[16])
                self._pc_exp_error(self.v_object_name, tmp, 0, es)
                v_new_reason = "%s新增场景" % v[11]
                v_sub_reason = "销户失败 未考虑到场景,手工确认"
                print(f"异常 zsf {es}")
                # v_sql = " update  ERR_NOINFO_STATISTICS t set t.reason1 = :v_sub_reason, t.reason  = " \
                #         ":v_new_reason where rowid = :rowid"
                # if v_sub_reason is None:
                #     v_sub_reason = ''
                # if v_new_reason is None:
                #     v_new_reason = ''
                # param = {'v_sub_reason': v_sub_reason,
                #          'v_new_reason': v_new_reason,
                #          'rowid': v[16]}
                # self.err_db.execute(v_sql, param, self.err_cur)
                # self.err_conn.commit()
            v_sql = " update  ERR_NOINFO_STATISTICS t set t.reason1 = :v_sub_reason, t.reason  = " \
                    ":v_new_reason where rowid = :rowid"
            if v_sub_reason is None:
                v_sub_reason = ''
            if v_new_reason is None:
                v_new_reason = ''
            param = {'v_sub_reason': v_sub_reason,
                     'v_new_reason': v_new_reason,
                     'rowid': v[16]}
            self.err_db.execute(v_sql, param, self.err_cur)
            self.err_conn.commit()
        print("结束循环 ")

    def _p_check_group_data(self, i_msisdn, i_area_code, i_cdr_min_start_time, i_cdr_max_time):
        n_member_type = None
        if i_msisdn is None:
            i_msisdn = 10086
        tmp = '0' + str(i_msisdn)
        v_sql = "select decode(substr(:i_msisdn, 1, 1), '0', :i_msisdn,:tmp) from dual"
        param = {'i_msisdn': i_msisdn,
                 'tmp': tmp}
        self.err_db.execute(v_sql, param, self.err_cur)
        v_msisdn_data = self.err_cur.fetchone()
        if v_msisdn_data is None:
            v_msisdn = '0'
        else:
            v_msisdn = v_msisdn_data[0]
        if i_area_code is None:
            i_area_code = 0
        v_sql = "select t.db_link   from err_cc_city_info t where t.city_code = " \
                "trim(:i_area_code)"
        param = {'i_area_code': str(i_area_code)}
        self.err_db.execute(v_sql, param, self.err_cur)
        v_dblink_data = self.err_cur.fetchone()
        if v_dblink_data is None:
            v_dblink = 1
        else:
            v_dblink = v_dblink_data[0]
        tmp_cur = None
        tmp_db = None
        tmp_conn = None
        if str(v_dblink).rstrip().endswith("1"):
            tmp_cur = self.account_cur
            tmp_conn = self.account_conn
            tmp_db = self.account_db
        elif str(v_dblink).rstrip().endswith("2"):
            tmp_cur = self.account2_cur
            tmp_conn = self.account2_conn
            tmp_db = self.account2_db
        elif str(v_dblink).rstrip().endswith("3"):
            tmp_cur = self.account3_cur
            tmp_conn = self.account3_conn
            tmp_db = self.account3_db
        elif str(v_dblink).rstrip().endswith("4"):
            tmp_cur = self.account4_cur
            tmp_conn = self.account4_conn
            tmp_db = self.account4_db
        if i_cdr_min_start_time is None or i_cdr_min_start_time == '0':
            i_cdr_min_start_time = '19710101245959'
        if i_cdr_max_time is None or i_cdr_max_time == '0':
            i_cdr_max_time = '20991231245959'
        v_sql = "select  count(1) , count(case when start_date <" \
                " to_date(:i_cdr_min_start_time, 'yyyymmddhh24miss') and nvl(end_date, " \
                "to_date('20991231', 'yyyymmdd')) > to_date(:i_cdr_max_time, 'yyyymmddhh24miss')" \
                " then 1 end) , count(case when start_date > to_date(:i_cdr_min_start_time," \
                " 'yyyymmddhh24miss') and nvl(end_date, to_date('20991231', 'yyyymmdd')) > start_date " \
                "then 1 end)  from group_member where " \
                "member_type in (6,11) and service_number=:v_msisdn"
        param = {'i_cdr_min_start_time': str(i_cdr_min_start_time),
                 'i_cdr_max_time': str(i_cdr_max_time),
                 'v_msisdn': str(v_msisdn)}
        tmp_db.execute(v_sql, param, tmp_cur)
        tmp_data = tmp_cur.fetchone()
        if tmp_data is None:
            n_total_cnt = 0
            n_cdr_inure_cnt = 0
            n_uninure_cnt = 0
        else:
            n_total_cnt = tmp_data[0]
            n_cdr_inure_cnt = tmp_data[1]
            n_uninure_cnt = tmp_data[2]
        if int(n_total_cnt) == 0:
            self.v_reson = "无资料"
        elif int(n_cdr_inure_cnt) > 1:
            self.v_reson = "资料异常 存在多条生效资料"
        elif int(n_uninure_cnt) > 0:
            self.v_reson = "资料未生效"
        elif int(n_cdr_inure_cnt) == 1:
            self.v_reson = "数据正常 需要进一步核实"
        elif int(n_cdr_inure_cnt) == 0:
            self.v_reson = "资料已失效"
        else:
            self.v_reson = "其他情况"
        v_sql = "select min(end_date) keep(dense_rank first order by end_date desc nulls first)," \
                " min(grp_user_id) keep(dense_rank first order by end_date desc nulls first) " \
                "from group_member " \
                "where member_type in (6,11) and service_number=:v_msisdn"
        param = {'v_msisdn': str(v_msisdn)}
        tmp_db.execute(v_sql, param, tmp_cur)
        tmp_data = tmp_cur.fetchone()
        if tmp_data is None:
            self.o_end_data = None
            self.o_data_user_id = 0
        else:
            self.o_end_data = tmp_data[0]
            self.o_data_user_id = tmp_data[1]
        if self.v_reson in '无资料':
            v_sql = "select min(end_date) keep(dense_rank first order by end_date desc nulls first)," \
                    " min(grp_user_id) keep(dense_rank first order by end_date desc nulls first), " \
                    "min(member_type) keep(dense_rank first order by end_date desc nulls first), " \
                    "min(product_id) keep(dense_rank first order by end_date desc nulls first) " \
                    "from group_member where  product_id not in (1110000026)" \
                    " and service_number=:v_msisdn"
            param = {'v_msisdn': str(v_msisdn)}
            tmp_db.execute(v_sql, param, tmp_cur)
            tmp_data = tmp_cur.fetchone()
            if tmp_data is None:
                self.o_end_data = None
                self.o_data_user_id = 0
                n_member_type = 0
                n_product_id = 0
            else:
                self.o_end_data = tmp_data[0]
                self.o_data_user_id = tmp_data[1]
                n_member_type = tmp_data[2]
                n_product_id = tmp_data[3]
            if self.o_data_user_id is not None or self.o_data_user_id == 0:
                self.v_reson = "群组表中的member_type和product_id非计费资料"
                self.v_reson_remark = "加入的群组product_id 为 %smember_type为%s" % (n_product_id, n_member_type)

    def _fn_get_proc_seq(self):
        print("开始生成日期戳")
        v_sql = "select to_char(sysdate, 'yyyymmddhh24miss') || trim(to_char(SEQ_ERR_proc_ACT.nextval, '0000000009'))" \
                "  from dual"
        try:
            self.err_cur.execute(v_sql)
            k_data = self.err_cur.fetchone()
            k = k_data[0]
            return k
        except Exception as e:
            print(f"时间转换{e}")
            return 0

    def _pc_act_start(self, i_step, i_object_name, i_info):
        if i_info is None:
            i_info = ''
        print("开始生成日志")
        o_seq = self._fn_get_proc_seq()
        v_sql = " insert into err_proc_action_log (seq,object_name,step,begin_date,status, remark,run_second,end_date)" \
                " values ('%s', upper(trim('%s')), '%s', sysdate, 1, trim('%s  正在执行'),'','')" % (o_seq,
                                                                                                 i_object_name, i_step,
                                                                                                 i_info)
        print(f"sql is {v_sql}")
        try:
            self.err_cur.execute(v_sql)
            self.err_conn.commit()
            return o_seq
        except Exception as e:
            print(f"pc act start error {e}")

    def _pc_act_end(self, i_seq, i_step, i_object_name, i_info, i_status):
        print(f"执行结束日志{i_info}")
        v_sql = "update err_proc_action_log t set t.end_date   = sysdate, t.run_second =" \
                " trunc((nvl(t.end_date, sysdate) - nvl(t.begin_date, sysdate)) * 24 * 60 * 60), t.remark   " \
                "  = '%s' || ' 处理结束', t.status     = '%s' where t.seq = '%s' and t.step = '%s'" % (i_info, i_status,
                                                                                                   i_seq, i_step)
        try:
            self.err_cur.execute(v_sql)
            self.err_conn.commit()
        except Exception as e:
            print(f"pc act end 异常{e}")

    def _pc_exp_error(self, i_object_name, i_info, i_proc_action_seq, e):
        print("异常生成")
        k = self._fn_get_proc_seq()
        v_sql_errm = "' sqlcode=' || SQLCODE || '; sqlerrm=' || SUBSTR(SQLERRM, 1, 200) || '; sql_location=' || " \
                     "dbms_utility.format_error_backtrace()"
        v_sql = "insert into err_proc_error_log t (seq, object_name, error_date, error_info, proc_action_seq) values" \
                "('%s',upper(trim('%s')),sysdate,'%s || %s','%s')" % (
                    k, i_object_name, i_info, str(e).replace("\'", "\'\'"),
                    i_proc_action_seq)
        print(f"err sql {v_sql}")
        try:
            self.err_cur.execute(v_sql)
            self.err_conn.commit()
        except Exception as e:
            print(f"error {e}")
            raise e

    def _main_deal(self):
        self._init_db()
        print("主流程开始执行。")
        n_step = 1
        d = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        v_month_number = self.i_date[4:6]
        v_day_number = self.i_date[6:8]
        self.v_info = '统计错单计费号码话单量'
        o_seq = self._pc_act_start(n_step, self.v_object_name, self.v_info)
        v_sql = "delete from  ERR_NOINFO_STATISTICS where cdr_date=:i_date"
        print(f"delete sql {v_sql}")
        mark_sql = "select count(1) from error_E2314_oper_control a where a.cdr_date=:i_date"
        param = {'i_date': self.i_date}
        self.err_db.execute(mark_sql, param, self.err_cur)
        v_cnt_data = self.err_cur.fetchone()
        v_cnt = 0
        if v_cnt_data is None:
            v_cnt = 0
        else:
            v_cnt = v_cnt_data[0]
        if int(v_cnt) > 0:
            up_sql = "update error_E2314_oper_control a set a.data_ex=0 where a.cdr_date=:i_date"
            param = {'i_date': self.i_date}
            self.err_db.execute(up_sql, param, self.err_cur)
            self.err_conn.commit()
        else:
            in_sql = "insert into error.error_e2314_oper_control(cdr_date,data_ex,data_chk,data_art,file_ex,data_net) " \
                     "values(:i_date,0,0,0,0,0)"
            param = {'i_date': self.i_date}
            self.err_db.execute(in_sql, param, self.err_cur)
            self.err_conn.commit()
        try:
            n_step = 1
            param = {'i_date': self.i_date}
            self.err_db.execute(v_sql, param, self.err_cur)
            self.err_conn.commit()
            # 统计云化个人无主号码清单量
            v_sql = "insert into ERR_NOINFO_STATISTICS (cdr_date,error_code, source_type, msisdn,imsi ," \
                    "area_code, cdr_cnt,cdr_min_start_time ,cdr_max_end_time) select /*+parallel(a,8)*/ :i_date,'719'," \
                    " source_type, msisdn, imsi,city_code , count(1),min(start_date||start_time),max(nvl(end_date||" \
                    "end_time,to_char(to_date(start_date||start_time,'yyyymmddhh24miss') +1/24/3600*call_duration," \
                    "'yyyymmddhh24miss'))) from RECYCLE_ERROR_CDR  a where a.month_number=:v_month_number and" \
                    " a.day_number=:v_day_number and error_code in ('E2314','E2313') and source_type" \
                    " not in ('H', 'J', 'T') and regexp_like(a.start_time, '^[0-2][0-9][0-5][0-9][0-5][0-9]$') " \
                    "and (regexp_like(a.end_time, '^[0-2][0-9][0-5][0-9][0-5][0-9]$') or a.end_time is null) " \
                    "group by  :i_date, error_code,source_type, msisdn, city_code,imsi"
            print(f"sql {v_sql}")
            param = {'i_date': self.i_date,
                     'v_month_number': v_month_number,
                     'v_day_number': v_day_number,
                     'i_date': self.i_date}
            self.err_db.execute(v_sql, param, self.err_cur)
            self.err_conn.commit()
            # 统计品牌未找到的用户
            v_sql = "insert into ERR_NOINFO_STATISTICS (cdr_date,error_code, source_type, msisdn,imsi ," \
                    "area_code, cdr_cnt,cdr_min_start_time ,cdr_max_end_time) select /*+parallel(a,8)*/ " \
                    ":i_date,error_code, source_type, msisdn, imsi,city_code , count(1),min(start_date||start_time)" \
                    ",max(nvl(end_date||end_time,to_char(to_date(start_date||start_time,'yyyymmddhh24miss') " \
                    "+1/24/3600*call_duration,'yyyymmddhh24miss'))) from RECYCLE_ERROR_CDR  a where " \
                    "a.month_number=:v_month_number and a.day_number=:v_day_number and error_code = 'E2408' " \
                    "and source_type not in ('H', 'J', 'T') and regexp_like(a.start_time," \
                    " '^[0-2][0-9][0-5][0-9][0-5][0-9]$') and (regexp_like(a.end_time, " \
                    "'^[0-2][0-9][0-5][0-9][0-5][0-9]$') or a.end_time is null) group by " \
                    " :i_date, error_code,source_type, msisdn, city_code,imsi"
            print(f"sql {v_sql}")
            param = {'i_date': self.i_date,
                     'v_month_number': v_month_number,
                     'v_day_number': v_day_number}
            self.err_db.execute(v_sql, param, self.err_cur)
            self.err_conn.commit()
            # 统计流原生个人无主号码清单量
            v_sql = " insert into ERR_NOINFO_STATISTICS (cdr_date,error_code, source_type, msisdn,imsi ," \
                    "area_code, cdr_cnt,cdr_min_start_time ,cdr_max_end_time) select /*+parallel(a,8)*/ :i_date,'719'" \
                    ", source_type, msisdn, imsi,city_id , count(1),min(start_date||start_time)," \
                    "max(nvl(end_date||end_time,to_char(to_date(start_date||start_time,'yyyymmddhh24miss') " \
                    "+1/24/3600*call_duration,'yyyymmddhh24miss'))) from pulsar_recycle_error_cdr " \
                    " a where a.month_number=:v_month_number and a.day=:v_day_number and error_code in " \
                    "('E2008','E2123') and source_type not in ('H', 'J', 'T') and regexp_like(a.start_time, " \
                    "'^[0-2][0-9][0-5][0-9][0-5][0-9]$') and (regexp_like(a.end_time, " \
                    "'^[0-2][0-9][0-5][0-9][0-5][0-9]$') or a.end_time is null) group by  :i_date, " \
                    "error_code,source_type, msisdn, city_id,imsi"
            print(f"sql {v_sql}")
            param = {'i_date': self.i_date,
                     'v_month_number': v_month_number,
                     'v_day_number': v_day_number}
            self.err_db.execute(v_sql, param, self.err_cur)
            self.err_conn.commit()
            # 统计流原生品牌未找到的用户
            v_sql = "insert into ERR_NOINFO_STATISTICS (cdr_date,error_code, source_type, msisdn,imsi ," \
                    "area_code, cdr_cnt,cdr_min_start_time ,cdr_max_end_time) select /*+parallel(a,8)*/ :i_date,'719'," \
                    " source_type, msisdn, imsi,city_id , count(1),min(start_date||start_time)," \
                    "max(nvl(end_date||end_time,to_char(to_date(start_date||start_time,'yyyymmddhh24miss') " \
                    "+1/24/3600*call_duration,'yyyymmddhh24miss'))) from pulsar_recycle_error_cdr " \
                    " a where a.month_number=:v_month_number and a.day=:v_day_number and error_code = 'E2020' and " \
                    "source_type not in ('H', 'J', 'T') and regexp_like(a.start_time, " \
                    "'^[0-2][0-9][0-5][0-9][0-5][0-9]$') and (regexp_like(a.end_time, " \
                    "'^[0-2][0-9][0-5][0-9][0-5][0-9]$') or a.end_time is null) group by  " \
                    ":i_date, error_code,source_type, msisdn, city_id,imsi"
            print(f"sql {v_sql}")
            param = {'i_date': self.i_date,
                     'v_month_number': v_month_number,
                     'v_day_number': v_day_number}
            self.err_db.execute(v_sql, param, self.err_cur)
            self.err_conn.commit()
            v_sql = "delete from f_product_service_code"
            try:
                print(f"错单库删除 f_product_service_code，sql is {v_sql}")
                self.err_cur.execute(v_sql)
                self.err_conn.commit()
                v_sql = "select distinct prodid,serviceid  from" \
                        " bossmnt.product_service t"
                print(f" pub tbcs sql is {v_sql}")
                self.pub_cur.execute(v_sql)
                tmp_data = self.pub_cur.fetchall()
                # 循环所有得到数据
                print(f"数据太多不展示代销{len(tmp_data)}")
                for d in tmp_data:
                    pro_id = d[0]
                    service_id = d[1]
                    v_sql = "select nvl(min(product_name),:pro_id) " \
                            " from product_cfg t where product_code =:service_id"
                    t_data = None
                    param = {'pro_id': str(service_id),
                             'service_id': str(pro_id)}
                    self.pub_db.execute(v_sql, param, self.pub_cur)
                    t_data = self.pub_cur.fetchone()
                    if t_data is not None:
                        product_name = t_data[0]
                        v_sql = "insert into f_product_service_code( PRODID,SERVICEID,PRODUCT_NAME)" \
                                " values(:pro_id,:service_id,:product_name)"
                        param = {'pro_id': str(pro_id),
                                 'service_id': str(d[1]),
                                 'product_name': str(product_name)}
                        self.err_db.execute(v_sql, param, self.err_cur)
                        self.err_conn.commit()
                print("执行完成")
                self.n_statue = 9
            except Exception as e:
                print(f"出现异常{e}")
                v_sql = "select prodid,serviceid " \
                        " from bossmnt.product_service t"
                print(f"sql is {v_sql}")
                self.pub_cur.execute(v_sql)
                tmp_data = self.pub_cur.fetchall()
                index = 0
                for d in tmp_data:
                    pro_id = d[0]
                    service_id = d[1]
                    v_sql = "select  nvl(min(product_name),:pro_id) " \
                            " from product_cfg t where product_code =:service_id"
                    print(f"插入sql is {v_sql}")
                    param = {'pro_id': str(pro_id),
                             'service_id': str(service_id)}
                    self.pub_db.execute(v_sql, param, self.pub_cur)
                    t_data = self.pub_cur.fetchone()
                    if t_data is not None:
                        if index < 1:
                            v_sql_c = "create table f_product_service_code(PRODID VARCHAR2(32) not nul," \
                                      " SERVICEID  VARCHAR2(16) not null, PRODUCT_NAME VARCHAR2(128))"
                            print(f"pub库创建表{v_sql_c}")
                            self.err_cur.execute(v_sql_c)
                            self.err_conn.commit()
                        v_sql = "insert into f_product_service_code( PRODID,SERVICEID,PRODUCT_NAME) " \
                                "values(?, ?, ?)"
                        self.err_db.execute(v_sql, (pro_id, service_id, t_data[0]), self.err_cur)
                        print(f"插f_product_service_code {v_sql}")
                    self.err_conn.commit()
                self.n_statue = -1
                print(f"异常{e}")
        except Exception as e:
            print(f"主程序数据库异常{e}")
            self._pc_exp_error(self.v_object_name, self.v_info, o_seq, e)
            self.n_statue = -1
            raise e
        finally:
            self._pc_act_end(o_seq, n_step, self.v_object_name, self.v_info, self.n_statue)
        n_step = 2
        self.v_info = '校验个人用户资料'
        print(f"开始执行{self.v_info}")
        tmp_info = '步骤' + o_seq + self.v_info
        o_seq = self._pc_act_start(n_step, self.v_object_name, tmp_info)
        n_flag = ''
        d_end_date = ''
        n_data_user_id = ''
        try:
            data = self._cur_msisdn_error719()
            print(f"获取得719数据大小{len(data)}")
            js = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            print(f"进去时间{js}")
            date_list = []
            v_sql = "update ERR_NOINFO_STATISTICS t set t.reason= :v_reson, t.reason1= :v_reson_remark," \
                    " t.remark= :v_remark,t.inure_data_cnt = :o_see_flag, t.end_date=to_date(:tmp_end_date,'yyyyMMddHH24miss')," \
                    " t.user_id=:o_data_user_id where t.rowid = :rowid"
            for v in data:
                self.v_reson_remark = ''
                self.v_reson = ''
                self.v_remark = ''
                try:
                    self._p_check_personal_data(v[3], v[4], v[9], v[10])
                except Exception as e:
                    print(f"异常 {e}， misi{v[3]} ,areacode{v[4]},begin{v[9]},end{v[10]}")
                    raise e
                if self.o_end_data == 0:
                    # tmp_end_date = self._date_formart(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    tmp_end_date = None
                else:
                    tmp_end_date = self._date_formart(str(self.o_end_data))
                date_list.append({'v_reson': self.v_reson,
                                  'v_reson_remark': self.v_reson_remark,
                                  'v_remark': self.v_remark,
                                  'o_see_flag': self.o_see_flag,
                                  'tmp_end_date': tmp_end_date,
                                  'o_data_user_id': self.o_data_user_id,
                                  'rowid': v[16]})
                if len(date_list) >= 2000:
                    print("批量执行开始")
                    self.err_db.executemany(v_sql, date_list, self.err_cur)
                    self.err_conn.commit()
                    time.sleep(0.01)
                    date_list.clear()
                    print("批量结束")
            if len(date_list) > 0:
                self.err_db.executemany(v_sql, date_list, self.err_cur)
                self.err_conn.commit()
        except Exception as e:
            print(f"719 sql {v_sql}")
            print(f"719 异常{e} ,v_reson {self.v_reson}, reson_remak{self.v_reson_remark},"
                  f", enddate{self.o_end_data}, userid{self.o_data_user_id}")
            raise e
        finally:
            en = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            print(f"结束时候时间{en}")
        try:
            print("走到这儿了")
            v_sql = "update ERR_NOINFO_STATISTICS t set t.reason = '和多号 z7话单' where cdr_date = :i_date " \
                    "and error_code = '719' and t.reason = '需手工核实资料' and nvl(t.remark, 1) like '%和多号%' " \
                    "and t.source_type = 'z7'"
            print(f"update z7{v_sql}")
            param = {'i_date': self.i_date}
            self.err_db.execute(v_sql, param, self.err_cur)
            self.err_conn.commit()
            v_sql = "update ERR_NOINFO_STATISTICS t set t.reason = '业务不应该开通相关功能' " \
                    "where cdr_date = :i_date and error_code='719' and t.reason='需手工核实资料'" \
                    " and ((nvl(t.remark, 1) like '%多号%' and t.source_type " \
                    "not in ('A', 'E','69','H','J','T','N')) or (nvl(t.remark, 1) " \
                    "IN ('捆绑随E行', '4G手机随e行（免功能费）', '血压通') AND T.SOURCE_TYPE " \
                    "NOT IN ('Q', 'M')) or (nvl(t.remark, 1) IN ('H-ZONE业务') AND T.SOURCE_TYPE" \
                    " not IN ('H', 'J')))"
            print(f"update 719 {v_sql}")
            param = {'i_date': self.i_date}
            self.err_db.execute(v_sql, param, self.err_cur)
            self.err_conn.commit()
            en = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            print(f"_sp_analyze_terminal_user分析开始i时间{en}")
            self._sp_analyze_terminal_user()
            en = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            print(f"_sp_analyze_terminal_user分析结束时间{en}")
            self.n_statue = 9
        except Exception as e:
            tmp = "步骤" + str(n_step) + self.v_info
            self._pc_exp_error(self.v_object_name, o_seq, tmp, e)
            self.n_statue = -1
            print(f"异常捕获{e}")
            raise e
        finally:
            mp = "步骤" + str(n_step) + self.v_info
            self._pc_act_end(o_seq, n_step, self.v_object_name, mp, self.n_statue)
        try:
            n_step = 3
            self.v_info = '715无主统计'
            o_seq = self._pc_act_start(n_step, self.v_object_name, self.v_info)
            v_sql = "insert into ERR_NOINFO_STATISTICS (cdr_date,error_code, source_type, msisdn, " \
                    "area_code, cdr_cnt,cdr_duration,cdr_min_start_time ,cdr_max_end_time) select /*+parallel(a,8)*/" \
                    " :i_date,'715', source_type, msisdn, CITY_ID , count(1),sum(ceil(call_duration/60))," \
                    "min(start_date||start_time),max(nvl(end_date||end_time,to_char(to_date(start_date||start_time," \
                    "'yyyymmddhh24miss') +1/24/3600*call_duration,'yyyymmddhh24miss'))) from PULSAR_RECYCLE_ERROR_CDR " \
                    "a where a.month_number=:v_month_number and a.day_number=:v_day_number and error_code = 'E2059'" \
                    " group by :i_date, error_code,source_type, msisdn, CITY_ID"
            param = {'i_date': self.i_date,
                     'v_month_number': int(v_month_number),
                     'v_day_number': int(v_day_number)}
            self.err_db.execute(v_sql, param, self.err_cur)
            self.err_conn.commit()
            self.n_statue = 9
        except Exception as e:
            self._pc_exp_error(self.v_object_name, self.v_info, o_seq, e)
            self.n_statue = -1
            raise e
        finally:
            self._pc_act_end(o_seq, n_step, self.v_object_name, self.v_info, self.n_statue)
        # 校验集团用户资料
        n_step = 4
        self.v_info = '校验集团用户资料'
        o_seq = self._pc_act_start(n_step, self.v_object_name, self.v_info)
        try:
            data = self._cur_msisdn_error715()
            print(f"715 数据大小{len(data)}")
            date_list = []
            v_sql = "update ERR_NOINFO_STATISTICS t set t.reason =:v_reson ,t.reason1   " \
                    "  = :v_reson_remark, t.inure_data_cnt =:o_see_flag, t.end_date=to_date(:o_end_data,'yyyymmddhh24miss')" \
                    ", t.user_id=:o_data_user_id " \
                    " where t.rowid = :rowid"
            for v in data:
                try:
                    self.v_reson_remark = ''
                    self.v_reson = ''
                    self.o_end_data = '20991231245959'
                    self._p_check_group_data(v[3], v[4], v[9], v[10])
                    end_date = None
                    user_id = None
                    if self.o_end_data is None or self.o_data_user_id is None:
                        end_date = None
                        user_id = 0
                    else:
                        end_date = self._date_formart(str(self.o_end_data))
                        user_id = self.o_data_user_id
                    date_list.append({'v_reson': self.v_reson,
                                      'v_reson_remark': self.v_reson_remark,
                                      'o_see_flag': self.o_see_flag,
                                      'o_end_data': end_date,
                                      'o_data_user_id': user_id,
                                      'rowid': str(v[16])})
                    if len(date_list) >= 1000:
                        print(f"批量执行开始 {v_sql} ")
                        self.err_db.executemany(v_sql, date_list, self.err_cur)
                        self.err_conn.commit()
                        time.sleep(0.01)
                        date_list.clear()
                        print(f"批量执行结束 {v_sql}")
                except Exception as ex:
                    print(f"yichang {ex}---{v_sql}---{date_list}")
            if len(date_list) > 0:
                self.err_db.executemany(v_sql, date_list, self.err_cur)
                print(f"执行sql {v_sql}")
                self.err_conn.commit()
            self.n_statue = 9
        except Exception as e:
            self._pc_exp_error(self.v_object_name, self.v_info, o_seq, e)
            self.n_statue = -1
            print(f"err is {e}")
        finally:
            print(f" finall o_seq {o_seq} n_step{n_step}")
            self._pc_act_end(o_seq, n_step, self.v_object_name, self.v_info, self.n_statue)
            print(f"o_seq {o_seq} n_step{n_step}")
            self._pc_act_end(o_seq, n_step, self.v_object_name, self.v_info, self.n_statue)
            fi_sql = "update error_E2314_oper_control a set a.data_ex=1 where a.cdr_date=:i_date"
            param = {'i_date': self.i_date}
            self.err_db.execute(fi_sql, param, self.err_cur)
            self.err_conn.commit()
            time.sleep(0.1)
            # self._pc_act_end(o_seq, 0, self.v_object_name, '无主错单号码统计', self.n_statue)
            self._close_resource()

    def _date_formart(self, str_date):
        if str_date.find('-') != -1:
            timeArray = time.strptime(str(str_date), "%Y-%m-%d %H:%M:%S")
            timeStamp = int(time.mktime(timeArray))
            otherStyleTime = time.strftime("%Y%m%d%H%M%S", timeArray)
            return otherStyleTime
        else:
            return str_date

    def _close_resource(self):
        print("准备关闭所有数据源")
        try:
            if self.err_cur:
                self.err_cur.close()
            if self.err_conn:
                self.err_conn.close()
            if self.account_cur:
                self.account_cur.close()
            if self.account_conn:
                self.account_conn.close()
            if self.account2_cur:
                self.account2_cur.close()
            if self.account2_conn:
                self.account2_conn.close()
            if self.account3_cur:
                self.account3_cur.close()
            if self.account3_conn:
                self.account3_conn.close()
            if self.account4_cur:
                self.account4_cur.close()
            if self.account4_conn:
                self.account4_conn.close()
            if self.sps_cur:
                self.sps_cur.close()
            if self.sps_conn:
                self.sps_conn.close()
            if self.sps_cur2:
                self.sps_cur2.close()
            if self.sps_conn2:
                self.sps_conn2.close()
            if self.sps_cur3:
                self.sps_cur3.close()
            if self.sps_conn3:
                self.sps_conn3.close()
            if self.sps_cur4:
                self.sps_cur4.close()
            if self.sps_conn4:
                self.sps_conn4.close()
            if self.pub_cur:
                self.pub_cur.close()
            if self.pub_conn:
                self.pub_conn.close()
        except Exception as e:
            print(f"关闭数据源异常{e}")
        print("关闭所有数据源完成")


if __name__ == '__main__':
    sp_error = sp_error_noInfo_statistics()
    try:
        sp_error._main_deal()
    except Exception as e:
        print(f"error ================{e}")
        raise e
