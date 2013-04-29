// Github: http://www.github.com/laufin
// Licensed: LGPL-2.1 <http://opensource.org/licenses/lgpl-2.1.php>
// Author: Laufin <laufin@qq.com>
// Copyright (c) 2012-2013 Laufin,all rights reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Web;
using System.Text.RegularExpressions;

namespace Ling.Dber
{
    /// <summary>
    /// 数据库表操作类
    /// Database table helper class
    /// </summary>
    public partial class DbTable : DbHelper
    {
        /// <summary>
        /// 表名
        /// </summary>
        protected string _tableName = string.Empty;
        /// <summary>
        /// 字段信息
        /// </summary>
        private Dictionary<string, TableFieldSchemeInfo> _fieldSchemeInfoList = null;

        /// <summary>
        /// SELECT
        /// </summary>
        protected string _select = string.Empty;
        /// <summary>
        /// TOP
        /// </summary>
        protected int _top = 0;
        /// <summary>
        /// WHERE
        /// </summary>
        protected string _where = string.Empty;
        /// <summary>
        /// ORDER
        /// </summary>
        protected string _order = string.Empty;
        /// <summary>
        /// 参数列表
        /// </summary>
        protected List<SqlParameter> _sqlParameterList = null;
        /// <summary>
        /// 是否使用
        /// </summary>
        protected bool _isSqlParamMode = true;

        /// <summary>
        /// 设置
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="connectionString"></param>
        /// <param name="isSqlParamMode"></param>
        /// <param name="tableSchemeCacheTime"></param>
        public void Setting(string tableName, string connectionString, bool isSqlParamMode = true, int tableSchemeCacheTime = 60)
        {
            _connstring = connectionString;
            _isSqlParamMode = isSqlParamMode;
            _tableName = tableName;
            if (isSqlParamMode)
            {
                string dbName = "DATABASE";
                Match match = Regex.Match(_connstring, @"((Initial\s+Catalog)|(Database))\s*=\s*(?<dbname>[^;$]+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
                if (match.Success)
                {
                    dbName = match.Groups["dbname"].Value;
                }
                string cacheKey = "LING_DBER_TABLE_" + dbName + "_" + _tableName;

                //加载表结构
                if (tableSchemeCacheTime > 0 && HttpRuntime.Cache[cacheKey] != null)
                {
                    _fieldSchemeInfoList = HttpRuntime.Cache[cacheKey] as Dictionary<string, TableFieldSchemeInfo>;
                }
                else
                {
                    _fieldSchemeInfoList = _LoadFieldSchemeInfoList();
                    if (tableSchemeCacheTime > 0 && _fieldSchemeInfoList != null)
                    {
                        HttpRuntime.Cache.Insert(cacheKey, _fieldSchemeInfoList, null, DateTime.Now.AddSeconds(tableSchemeCacheTime), TimeSpan.Zero);
                    }
                }
            }
        }


        #region __私有方法__

        /// <summary>
        /// 数据库模式
        /// </summary>
        /// <returns></returns>
        private Dictionary<string, TableFieldSchemeInfo> _LoadFieldSchemeInfoList()
        {
            Dictionary<string, TableFieldSchemeInfo> list = null;
            string sql = @"SELECT TABLE_CATALOG, TABLE_NAME,COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH  
                            FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + _tableName + "'";
            DataTable dtList = ExecuteDataTable(CommandType.Text, sql);

            if (dtList != null && dtList.Rows.Count > 0)
            {
                list = new Dictionary<string, TableFieldSchemeInfo>();
                foreach (DataRow row in dtList.Rows)
                {
                    TableFieldSchemeInfo info = new TableFieldSchemeInfo();
                    info.ColumnName = row["COLUMN_NAME"].ToString();
                    info.SqlDbType = Utility.SqlTypeString2SqlType(row["DATA_TYPE"].ToString());
                    int size = 0;
                    try
                    {
                        size = Convert.ToInt32(row["CHARACTER_MAXIMUM_LENGTH"]);
                    }
                    catch { }
                    info.MaxSize = size;
                    string lowColumenName = info.ColumnName.ToLower();
                    if (!list.ContainsKey(lowColumenName))
                    {
                        list.Add(lowColumenName, info);
                    }
                }
            }
            return list;
        }

        /// <summary>
        /// 释放资源和条件
        /// </summary>
        protected void _ReleaseQueryConditon()
        {
            _select = string.Empty;
            _top = 0;
            _where = string.Empty;
            _sqlParameterList = null;
            _order = string.Empty;
        }

        #endregion

        #region __条件方法__

        /// <summary>
        /// where条件
        /// </summary>
        /// <param name="condition"></param>
        /// <returns></returns>
        public DbTable Where(string condition)
        {
            SqlParameter[] sqlParameters = null;
            return Where(condition, sqlParameters);
        }

        /// <summary>
        /// where条件
        /// </summary>
        /// <param name="condition">条件</param>
        /// <param name="sqlParameterModel">参数实体</param>
        /// <returns></returns>
        public DbTable Where(string condition, DbModel sqlParameterModel)
        {
            _where = condition;
            if (sqlParameterModel != null)
            {
                Dictionary<string, object> attrList = sqlParameterModel.GetAttrList();
                foreach (string key in attrList.Keys)
                {
                    if (_sqlParameterList == null)
                    {
                        _sqlParameterList = new List<SqlParameter>();
                    }
                    string fieldKey = key.Trim().ToLower();
                    if (_fieldSchemeInfoList != null && _fieldSchemeInfoList.ContainsKey(fieldKey))
                    {
                        TableFieldSchemeInfo fieldInfo = _fieldSchemeInfoList[fieldKey];
                        _sqlParameterList.Add(fieldInfo.CreateSqlParamter(attrList[key]));
                    }
                    else
                    {
                        SqlParameter newParameter = new SqlParameter();
                        newParameter.ParameterName = key;
                        newParameter.SqlDbType = Utility.CsharpType2SqlDbType(attrList[key].GetType());
                        newParameter.Value = attrList[key];
                        _sqlParameterList.Add(newParameter);
                    }
                }
            }
            return this;
        }

        /// <summary>
        /// where条件
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="sqlParameters"></param>
        /// <returns></returns>
        public DbTable Where(string condition, params SqlParameter[] sqlParameters)
        {
            _where = condition;
            if (sqlParameters != null && sqlParameters.Length > 0)
            {
                if (_sqlParameterList == null)
                {
                    _sqlParameterList = new List<SqlParameter>();
                }
                foreach (SqlParameter parameter in sqlParameters)
                {
                    _sqlParameterList.Add(parameter);
                }
            }
            return this;
        }

        /// <summary>
        /// 选择
        /// </summary>
        /// <param name="fields">字段</param>
        /// <returns></returns>
        public DbTable Select(string fields)
        {
            _select = fields;
            return this;
        }

        /// <summary>
        /// 选取几条记录
        /// </summary>
        /// <param name="count">条数</param>
        /// <returns></returns>
        public DbTable Top(int count)
        {
            _top = count;
            return this;
        }

        /// <summary>
        /// 排序
        /// </summary>
        /// <param name="sort">排序字段</param>
        /// <returns></returns>
        public DbTable OrderBy(string sort)
        {
            _order = sort;
            return this;
        }

        #endregion


        #region __执行操作__

        /// <summary>
        /// 返回DataTable数据
        /// </summary>
        /// <returns></returns>
        public DataTable DataTable()
        {
            return DataTable(0);
        }

        /// <summary>
        /// 返回DataTable数据
        /// </summary>
        /// <param name="timeout">超时时间</param>
        /// <returns></returns>
        public DataTable DataTable(int timeout)
        {
            try
            {
                StringBuilder sql = new StringBuilder();
                sql.Append(" SELECT ");
                if (_top > 0)
                {
                    sql.Append(" TOP " + _top + " ");
                }
                if (string.IsNullOrEmpty(_select))
                {
                    _select = "*";
                }
                sql.Append(" " + _select + " ");
                sql.Append(" FROM  " + _tableName + " ");
                if (!string.IsNullOrEmpty(_where))
                {
                    sql.Append(" WHERE " + _where + " ");
                }
                if (!string.IsNullOrEmpty(_order))
                {
                    sql.Append(" ORDER BY " + _order + " ");
                }

                SqlParameter[] sqlParams = _sqlParameterList == null ? null : _sqlParameterList.ToArray();
                return ExecuteDataTable(CommandType.Text, sql.ToString(), sqlParams, timeout);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                _ReleaseQueryConditon();   //释放资源
            }
        }

        /// <summary>
        /// 返回List列表
        /// </summary>
        /// <returns></returns>
        public List<DbModel> List(int timeout = 0)
        {
            DataTable dtList = DataTable(timeout);
            if (dtList != null)
            {
                return ToDbModel(dtList);
            }
            return null;
        }

        /// <summary>
        /// 列表实体
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public List<T> List<T>(int timeout = 0) where T : class, new()
        {
            DataTable dtList = DataTable(timeout);
            if (dtList != null)
            {
                return ToModel<T>(dtList);
            }
            return null;
        }

        /// <summary>
        /// 分页实体
        /// </summary>
        /// <param name="pageIndex"></param>
        /// <param name="pageSize"></param>
        /// <param name="records"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public List<T> PageList<T>(int pageIndex, int pageSize, out int records, int timeout = 0) where T : class, new()
        {
            DataTable dtList = PageTable(pageIndex, pageSize, out records, timeout);
            if (dtList != null)
            {
                return DbTable.ToModel<T>(dtList);
            }
            return null;
        }

        /// <summary>
        /// 分页列表
        /// </summary>
        /// <param name="pageIndex"></param>
        /// <param name="pageSize"></param>
        /// <param name="records"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public List<DbModel> PageList(int pageIndex, int pageSize, out int records, int timeout = 0)
        {
            DataTable dtList = PageTable(pageIndex, pageSize, out records, timeout);
            if (dtList != null)
            {
                return DbTable.ToDbModel(dtList);
            }
            return null;
        }

        /// <summary>
        /// 分页列表
        /// </summary>
        /// <param name="pageIndex">当前页，1开始</param>
        /// <param name="pageSize">每页记录数</param>
        /// <param name="records">总记录数（输出）</param>
        /// <param name="timeout">超时时间</param>
        /// <returns></returns>
        public DataTable PageTable(int pageIndex, int pageSize, out int records, int timeout = 0)
        {
            try
            {
                StringBuilder sql = new StringBuilder();
                sql.Append(" SELECT ");
                if (_top > 0)
                {
                    sql.Append(" TOP " + _top + " ");
                }
                if (string.IsNullOrEmpty(_select))
                {
                    _select = "*";
                }
                sql.Append(" " + _select + " ");
                sql.Append(" FROM  " + _tableName + " ");
                if (!string.IsNullOrEmpty(_where))
                {
                    sql.Append(" WHERE " + _where + " ");
                }

                //string order = string.Empty;
                //if (!string.IsNullOrEmpty(_order))
                //{
                //    order = " ORDER BY " + _order + " ";
                //}
                SqlParameter[] sqlParams = _sqlParameterList == null ? null : _sqlParameterList.ToArray();
                return ExecutePageTable(sql.ToString(), sqlParams, _order, pageIndex, pageSize, out records, timeout);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                _ReleaseQueryConditon();   //释放资源
            }
        }

        /// <summary>
        /// 计算记录数
        /// </summary>
        /// <returns></returns>
        public int Count()
        {
            return Count(0);
        }

        /// <summary>
        /// 计算记录数
        /// </summary>
        /// <param name="timeout">超时时间</param>
        /// <returns></returns>
        public int Count(int timeout)
        {
            try
            {
                StringBuilder sqlBuider = new StringBuilder();
                sqlBuider.Append(" SELECT COUNT(0) FROM " + _tableName);
                if (!string.IsNullOrEmpty(_where))
                {
                    sqlBuider.Append(" WHERE " + _where);
                }
                SqlParameter[] sqlParams = _sqlParameterList == null ? null : _sqlParameterList.ToArray();
                object objReturn = ExecuteScalar(CommandType.Text, sqlBuider.ToString(), sqlParams, timeout);
                if (objReturn != null && objReturn.ToString() != "")
                {
                    return Convert.ToInt32(objReturn);
                }
                return 0;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                _ReleaseQueryConditon();   //释放资源
            }
        }

        /// <summary>
        /// 查找
        /// </summary>
        /// <returns></returns>
        public DbModel Find()
        {
            DbModel model = null;
            try
            {
                _top = 1;
                List<DbModel> list = List();
                if (list != null && list.Count > 0)
                {
                    model = list[0];
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                _ReleaseQueryConditon();   //释放资源
            }
            return model;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public T Find<T>() where T : class, new()
        {
            T model = null;
            try
            {
                _top = 1;
                List<T> list = List<T>();
                if (list != null && list.Count > 0)
                {
                    model = list[0];
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                _ReleaseQueryConditon();   //释放资源
            }
            return model;
        }

        /// <summary>
        /// 插入数据
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        public int Add(DbModel model)
        {
            try
            {
                if (model == null || string.IsNullOrEmpty(_tableName))
                {
                    return 0;
                }
                Dictionary<string, object> attrList = model.GetAttrList();
                if (attrList == null || attrList.Count == 0)
                {
                    return 0;
                }
                StringBuilder strSql = new StringBuilder();
                StringBuilder strSql1 = new StringBuilder();
                StringBuilder strSql2 = new StringBuilder();
                foreach (string key in attrList.Keys)
                {
                    strSql1.Append(key + ",");
                    string fieldKey = key.Trim().ToLower();
                    if (_isSqlParamMode && _fieldSchemeInfoList != null && _fieldSchemeInfoList.ContainsKey(fieldKey))
                    {
                        strSql2.Append("@" + key + ",");
                        if (_sqlParameterList == null)
                        {
                            _sqlParameterList = new List<SqlParameter>();
                        }
                        TableFieldSchemeInfo fieldInfo = _fieldSchemeInfoList[fieldKey];
                        _sqlParameterList.Add(fieldInfo.CreateSqlParamter(attrList[key]));
                    }
                    else
                    {
                        if (attrList[key] == null)
                        {
                            strSql2.Append("NULL,");
                        }
                        else
                        {
                            strSql2.Append("'" + attrList[key] + "',");
                        }
                    }
                }
                strSql.Append("INSERT INTO " + _tableName + "(");
                strSql.Append(strSql1.ToString().Remove(strSql1.Length - 1));
                strSql.Append(")");
                strSql.Append(" VALUES (");
                strSql.Append(strSql2.ToString().Remove(strSql2.Length - 1));
                strSql.Append(")");
                strSql.Append(";SELECT SCOPE_IDENTITY() AS [ID]");

                SqlParameter[] sqlParams = _sqlParameterList == null ? null : _sqlParameterList.ToArray();
                object objRet = ExecuteScalar(CommandType.Text, strSql.ToString(), sqlParams);
                if (objRet != null && objRet.ToString() != "")
                {
                    return Convert.ToInt32(objRet);
                }
                return 0;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                _ReleaseQueryConditon();   //释放资源
            }
        }

        /// <summary>
        /// 更新内容
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        public int Update(DbModel model)
        {
            return Update(model, false);
        }

        /// <summary>
        /// 更新操作
        /// </summary>
        /// <param name="model">更新的内容</param>
        /// <param name="force">是否强制更新</param>
        /// <returns></returns>
        public int Update(DbModel model, bool force)
        {
            try
            {
                if (model == null || string.IsNullOrEmpty(_tableName))
                {
                    return 0;
                }
                Dictionary<string, object> attrList = model.GetAttrList();
                if (attrList == null || attrList.Count == 0)
                {
                    return 0;
                }

                StringBuilder strSql = new StringBuilder();
                strSql.Append("UPDATE " + _tableName + " SET ");
                //遍历更新字段
                foreach (string key in attrList.Keys)
                {
                    string fieldKey = key.Trim().ToLower();
                    if (_isSqlParamMode && _fieldSchemeInfoList != null && _fieldSchemeInfoList.ContainsKey(fieldKey))
                    {
                        strSql.Append(key + "=@" + key + ",");
                        if (_sqlParameterList == null)
                        {
                            _sqlParameterList = new List<SqlParameter>();
                        }
                        TableFieldSchemeInfo fieldInfo = _fieldSchemeInfoList[fieldKey];
                        _sqlParameterList.Add(fieldInfo.CreateSqlParamter(attrList[key]));
                    }
                    else
                    {
                        if (attrList[key] == null)
                        {
                            strSql.Append(key + "=NULL,");
                        }
                        else
                        {
                            strSql.Append(key + "='" + attrList[key] + "',");
                        }
                    }
                }
                int n = strSql.ToString().LastIndexOf(",");
                strSql.Remove(n, 1);
                if (string.IsNullOrEmpty(_where))
                {
                    if (!force)
                    {
                        return 0;
                    }
                }
                else
                {
                    strSql.Append(" WHERE " + _where + " ");
                }

                SqlParameter[] sqlParams = _sqlParameterList == null ? null : _sqlParameterList.ToArray();
                return ExecuteNonQuery(CommandType.Text, strSql.ToString(), sqlParams);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                _ReleaseQueryConditon();   //释放资源
            }
        }

        /// <summary>
        /// 删除操作，如果没有where条件，则需要force=true才会彻底删除
        /// </summary>
        /// <returns></returns>
        public int Delete()
        {
            return Delete(false);
        }


        /// <summary>
        /// 删除操作，如果没有where条件，则需要force=true才会彻底删除
        /// </summary>
        /// <param name="force">true=强制删除</param>
        /// <returns></returns>
        public int Delete(bool force)
        {
            try
            {
                if (string.IsNullOrEmpty(_tableName))
                {
                    return 0;
                }
                StringBuilder strSql = new StringBuilder();
                strSql.Append(" DELETE FROM " + _tableName);
                if (string.IsNullOrEmpty(_where))
                {
                    if (!force)
                    {
                        return 0;   //防止误删全部记录
                    }
                }
                else
                {
                    strSql.Append(" WHERE " + _where);
                }

                SqlParameter[] sqlParams = _sqlParameterList == null ? null : _sqlParameterList.ToArray();
                return ExecuteNonQuery(CommandType.Text, strSql.ToString(), sqlParams);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                _ReleaseQueryConditon();   //释放资源
            }
        }

        #endregion


        /// <summary>
        /// 表格转实体
        /// </summary>
        /// <param name="dtList"></param>
        /// <returns></returns>
        protected static List<DbModel> ToDbModel(DataTable dtList)
        {
            if (dtList == null || dtList.Rows.Count == 0)
                return null;
            List<DbModel> list = new List<DbModel>();
            foreach (DataRow row in dtList.Rows)
            {
                DbModel model = new DbModel();
                foreach (DataColumn col in dtList.Columns)
                {
                    string colName = col.ColumnName;
                    model[colName] = row[colName];
                }
                list.Add(model);
            }
            return list;
        }


        protected static List<T> ToModel<T>(DataTable dtList) where T : class, new()
        {
            if (dtList == null || dtList.Rows.Count == 0)
                return null;
            List<T> list = new List<T>();
            foreach (DataRow row in dtList.Rows)
            {
                DbModel model = new DbModel();
                foreach (DataColumn col in dtList.Columns)
                {
                    string colName = col.ColumnName;
                    model[colName] = row[colName];
                }
                list.Add(model.ToModel<T>());
            }
            return list;
        }

    }



}
