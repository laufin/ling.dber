// Github: http://www.github.com/laufin
// Licensed: LGPL-2.1 <http://opensource.org/licenses/lgpl-2.1.php>
// Author: Laufin <laufin@qq.com>
// Copyright (c) 2012-2013 Laufin,all rights reserved.

using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlClient;

namespace Ling.Dber
{

    /// <summary>
    /// 数据库辅助类
    /// Database helper class
    /// </summary>
    public class DbHelper
    {
        #region __静态方法__

        #region ExecuteReader
        /// <summary>
        /// ExecuteReader
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="commandType"></param>
        /// <param name="commandText"></param>
        /// <param name="sqlParameters"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public static SqlDataReader ExecuteReader(string connectionString, CommandType commandType, string commandText, SqlParameter[] sqlParameters = null, int timeout = 0)
        {
            SqlDataReader reader = null;
            using (SqlConnection sqlConn = new SqlConnection(connectionString))
            {
                sqlConn.Open();
                SqlCommand sqlCmd = new SqlCommand();
                sqlCmd.CommandText = commandText;
                sqlCmd.CommandType = commandType;
                sqlCmd.Connection = sqlConn;
                if (timeout > 0)
                {
                    sqlCmd.CommandTimeout = timeout;
                }
                if (sqlParameters != null)
                {
                    foreach (SqlParameter p in sqlParameters)
                    {
                        if (p != null)
                        {
                            if ((p.Direction == ParameterDirection.InputOutput ||
                                p.Direction == ParameterDirection.Input) &&
                                (p.Value == null))
                            {
                                p.Value = DBNull.Value;
                            }
                            sqlCmd.Parameters.Add(p);
                        }
                    }
                }
                reader = sqlCmd.ExecuteReader();
            }
            return reader;
        }

        #endregion

        #region ExecuteScalar
        /// <summary>
        /// ExecuteScalar
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="commandType"></param>
        /// <param name="commandText"></param>
        /// <param name="sqlParameters"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public static object ExecuteScalar(string connectionString, CommandType commandType, string commandText, SqlParameter[] sqlParameters = null, int timeout = 0)
        {
            object objReturn = 0;
            using (SqlConnection sqlConn = new SqlConnection(connectionString))
            {
                sqlConn.Open();
                SqlCommand sqlCmd = new SqlCommand();
                sqlCmd.CommandText = commandText;
                sqlCmd.CommandType = commandType;
                sqlCmd.Connection = sqlConn;
                if (timeout > 0)
                {
                    sqlCmd.CommandTimeout = timeout;
                }
                if (sqlParameters != null)
                {
                    foreach (SqlParameter p in sqlParameters)
                    {
                        if (p != null)
                        {
                            if ((p.Direction == ParameterDirection.InputOutput ||
                                p.Direction == ParameterDirection.Input) &&
                                (p.Value == null))
                            {
                                p.Value = DBNull.Value;
                            }
                            sqlCmd.Parameters.Add(p);
                        }
                    }
                }
                objReturn = sqlCmd.ExecuteScalar();
            }
            return objReturn;
        }
        #endregion

        #region ExecuteNonQuery
        /// <summary>
        /// 执行ExecuteNonQuery
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="commandType"></param>
        /// <param name="commandText"></param>
        /// <param name="sqlParameters"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public static int ExecuteNonQuery(string connectionString, CommandType commandType, string commandText, SqlParameter[] sqlParameters = null, int timeout = 0)
        {
            int iReturn = 0;
            using (SqlConnection sqlConn = new SqlConnection(connectionString))
            {
                sqlConn.Open();
                SqlCommand sqlCmd = new SqlCommand();
                sqlCmd.CommandText = commandText;
                sqlCmd.CommandType = commandType;
                sqlCmd.Connection = sqlConn;
                if (timeout > 0)
                {
                    sqlCmd.CommandTimeout = timeout;
                }
                if (sqlParameters != null)
                {
                    foreach (SqlParameter p in sqlParameters)
                    {
                        if (p != null)
                        {
                            if ((p.Direction == ParameterDirection.InputOutput ||
                                p.Direction == ParameterDirection.Input) &&
                                (p.Value == null))
                            {
                                p.Value = DBNull.Value;
                            }
                            sqlCmd.Parameters.Add(p);
                        }
                    }
                }
                iReturn = sqlCmd.ExecuteNonQuery();
            }
            return iReturn;
        }
        #endregion

        #region ExecuteDataset
        /// <summary>
        /// 执行DataSet
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="commandType"></param>
        /// <param name="commandText"></param>
        /// <param name="sqlParameters"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public static DataSet ExecuteDataset(string connectionString, CommandType commandType, string commandText, SqlParameter[] sqlParameters = null, int timeout = 0)
        {
            DataSet dsList = null;
            using (SqlConnection sqlConn = new SqlConnection(connectionString))
            {
                sqlConn.Open();
                SqlCommand sqlCmd = new SqlCommand();
                sqlCmd.CommandText = commandText;
                sqlCmd.CommandType = commandType;
                sqlCmd.Connection = sqlConn;
                if (timeout > 0)
                {
                    sqlCmd.CommandTimeout = timeout;
                }
                if (sqlParameters != null)
                {
                    foreach (SqlParameter p in sqlParameters)
                    {
                        if (p != null)
                        {
                            if ((p.Direction == ParameterDirection.InputOutput ||
                                p.Direction == ParameterDirection.Input) &&
                                (p.Value == null))
                            {
                                p.Value = DBNull.Value;
                            }
                            sqlCmd.Parameters.Add(p);
                        }
                    }
                }
                using (SqlDataAdapter da = new SqlDataAdapter(sqlCmd))
                {
                    if (dsList == null)
                    {
                        dsList = new DataSet();
                    }
                    da.Fill(dsList);
                }
            }

            return dsList;
        }
        #endregion

        #region ExecuteDataTable
        /// <summary>
        /// 执行DataTable
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="commandType"></param>
        /// <param name="commandText"></param>
        /// <param name="sqlParameters"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public static DataTable ExecuteDataTable(string connectionString, CommandType commandType, string commandText, SqlParameter[] sqlParameters = null, int timeout = 0)
        {
            DataSet dsList = ExecuteDataset(connectionString, commandType, commandText, sqlParameters, timeout);
            if (dsList != null && dsList.Tables.Count > 0)
            {
                return dsList.Tables[0];
            }
            return null;
        }
        #endregion

        #region ExecutePageTable

        /// <summary>
        /// 分页函数
        /// </summary>
        /// <param name="conectionString">连接字符串</param>
        /// <param name="sql">Sql语句</param>
        /// <param name="order">排序字段：ID ASC,Name DESC</param>
        /// <param name="pageIndex">分页索引</param>
        /// <param name="pageSize">分页方法</param>
        /// <param name="records">总记录数</param>
        /// <returns></returns>
        public static DataTable ExecutePageTable(string conectionString, string sql, string order, int pageIndex, int pageSize, out int records)
        {
            return ExecutePageTable(conectionString, sql, null, order, pageIndex, pageSize, out records, 0);
        }


        /// <summary>
        /// 分页函数
        /// </summary>
        /// <param name="conectionString">连接字符串</param>
        /// <param name="sql">Sql语句</param>
        /// <param name="sqlParameters">参数</param>
        /// <param name="order">排序字段：ID ASC,Name DESC</param>
        /// <param name="pageIndex">分页索引</param>
        /// <param name="pageSize">分页方法</param>
        /// <param name="records">总记录数</param>
        /// <returns></returns>
        public static DataTable ExecutePageTable(string conectionString, string sql, SqlParameter[] sqlParameters, string order, int pageIndex, int pageSize, out int records)
        {
            return ExecutePageTable(conectionString, sql, sqlParameters, order, pageIndex, pageSize, out records, 0);
        }

        /// <summary>
        /// 分页函数
        /// </summary>
        /// <param name="connectionString">连接字符串</param>
        /// <param name="sql">Sql语句</param>
        /// <param name="sqlParameters">参数</param>
        /// <param name="order">排序字段：ID ASC,Name DESC</param>
        /// <param name="pageIndex">分页索引</param>
        /// <param name="pageSize">分页方法</param>
        /// <param name="records">总记录数</param>
        /// <param name="timeout">超时时间</param>
        /// <returns></returns>
        public static DataTable ExecutePageTable(string connectionString, string sql, SqlParameter[] sqlParameters, string order, int pageIndex, int pageSize, out int records, int timeout)
        {
            records = 0;
            if (connectionString == null || connectionString.Length == 0)
            {
                return null;
            }

            DataTable dtList = null;
            if (pageIndex <= 0)
            {
                pageIndex = 1;
            }
            if (pageSize <= 0)
            {
                pageSize = 20;
            }

            StringBuilder tmpSql = new StringBuilder();
            //Table[0]总记录数
            tmpSql.Append(" SELECT COUNT(*) AS Records FROM  (" + sql + ") AS __LIUFULING_CN_TABLE0__; ");
            //Table[1]表数据
            if (!string.IsNullOrEmpty(order))
            {
                //构造排序并使用RowNumber进行排序
                tmpSql.Append(string.Format(@"
                          SELECT * FROM( 
	                            SELECT TOP {1} *, ROW_NUMBER() OVER(ORDER BY {0})  AS __LIUFULING_CN_NUMBER__ 
                                FROM ({2}) AS __LIUFULING_CN_TABLE1__
                                ORDER BY {0}
                            ) __LIUFULING_CN_TABLE2__
                            WHERE __LIUFULING_CN_NUMBER__ BETWEEN {3} AND {1}"
                    , order
                    , pageSize * pageIndex
                    , sql
                    , (pageIndex - 1) * pageSize + 1 //索引下标开始
                    ));

            }
            else
            {
                //构造排序并使用RowNumber进行排序
                tmpSql.Append(string.Format(@"
                            SELECT * FROM( 
	                            SELECT *, ROW_NUMBER() OVER(ORDER BY __LIUFULING_CN_FIELD__ ASC)  AS __LIUFULING_CN_NUMBER__ 
                                FROM (
		                            SELECT TOP {0} *, 0 AS __LIUFULING_CN_FIELD__ FROM ({1}) As __LIUFULING_CN_TABLE0__
	                            ) AS __LIUFULING_CN_TABLE1__
                            ) __LIUFULING_CN_TABLE2__
                            WHERE __LIUFULING_CN_NUMBER__ BETWEEN {2} AND {0}"
                    , pageSize * pageIndex
                    , sql
                    , (pageIndex - 1) * pageSize + 1 //索引下标开始
                    ));
            }

            DataSet dsList = ExecuteDataset(connectionString, CommandType.Text, tmpSql.ToString(), sqlParameters, timeout);
            if (dsList == null || dsList.Tables.Count < 2)
            {
                return null;
            }
            //总记录数
            records = Convert.ToInt32(dsList.Tables[0].Rows[0][0]);
            //表数据
            dtList = dsList.Tables[1];
            if (dtList != null)
            {
                //如果没带排序字段，则做事后移除字段处理
                if (dtList.Columns.Contains("__LIUFULING_CN_FIELD__"))
                {
                    dtList.Columns.Remove("__LIUFULING_CN_FIELD__");
                }
                if (dtList.Columns.Contains("__LIUFULING_CN_NUMBER__"))
                {
                    dtList.Columns.Remove("__LIUFULING_CN_NUMBER__");
                }
            }

            return dtList;
        }

        #endregion

        #endregion __静态方法__

        #region __实例方法__

        /// <summary>
        /// 连接字符串
        /// </summary>
        protected string _connstring = string.Empty;
        /// <summary>
        /// sqlConnection
        /// </summary>
        protected SqlConnection _sqlConnection = null;
        /// <summary>
        /// 事务
        /// </summary>
        protected SqlTransaction _sqlTrans = null;
        /// <summary>
        /// 是否使用事务标识
        /// </summary>
        protected bool _isTrans = false;
        /// <summary>
        /// 异常是否自动回滚事务
        /// </summary>
        protected bool _isExceptionAutoRollback = false;

        protected DbHelper()
        {
        }

        /// <summary>
        /// 实例方法支持事务处理
        /// </summary>
        protected DbHelper(string connectionstring)
        {
            this._connstring = connectionstring;
        }

        #region ExecuteReader
        /// <summary>
        /// ExecuteReader
        /// </summary>
        /// <param name="commandType"></param>
        /// <param name="commandText"></param>
        /// <param name="sqlParameters"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        protected SqlDataReader ExecuteReader(CommandType commandType, string commandText, SqlParameter[] sqlParameters = null, int timeout = 0)
        {
            return DbHelper.ExecuteReader(_connstring, commandType, commandText, sqlParameters, timeout);
        }
        #endregion

        #region ExecuteScalar
        /// <summary>
        /// ExecuteScalar
        /// </summary>
        /// <param name="commandType"></param>
        /// <param name="commandText"></param>
        /// <param name="sqlParameters"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        protected object ExecuteScalar(CommandType commandType, string commandText, SqlParameter[] sqlParameters = null, int timeout = 0)
        {
            if (_isTrans)
            {
                return _RunTransCommand<object>(commandType, commandText, sqlParameters, timeout, new _ExecuteCommand<object>(delegate(SqlCommand sqlCmd)
                {
                    return sqlCmd.ExecuteScalar();
                }));
            }
            else
            {
                return DbHelper.ExecuteScalar(_connstring, commandType, commandText, sqlParameters, timeout);
            }
        }
        #endregion

        #region ExecuteNonQuery
        /// <summary>
        /// 执行ExecuteNonQuery
        /// </summary>
        /// <param name="commandType"></param>
        /// <param name="commandText"></param>
        /// <param name="sqlParameters"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        protected int ExecuteNonQuery(CommandType commandType, string commandText, SqlParameter[] sqlParameters = null, int timeout = 0)
        {
            if (_isTrans)
            {
                return _RunTransCommand<int>(commandType, commandText, sqlParameters, timeout, new _ExecuteCommand<int>(delegate(SqlCommand sqlCmd)
                {
                    return sqlCmd.ExecuteNonQuery();
                }));
            }
            else
            {
                return DbHelper.ExecuteNonQuery(_connstring, commandType, commandText, sqlParameters, timeout);
            }
        }
        #endregion

        #region ExecuteDataset
        /// <summary>
        /// 执行DataSet
        /// </summary>
        /// <param name="commandType"></param>
        /// <param name="commandText"></param>
        /// <param name="sqlParameters"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        protected DataSet ExecuteDataset(CommandType commandType, string commandText, SqlParameter[] sqlParameters = null, int timeout = 0)
        {
            return DbHelper.ExecuteDataset(_connstring, commandType, commandText, sqlParameters, timeout);
        }
        #endregion

        #region ExecuteDataTable
        /// <summary>
        /// 执行DataTable
        /// </summary>
        /// <param name="commandType"></param>
        /// <param name="commandText"></param>
        /// <param name="sqlParameters"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        protected DataTable ExecuteDataTable(CommandType commandType, string commandText, SqlParameter[] sqlParameters = null, int timeout = 0)
        {
            return DbHelper.ExecuteDataTable(_connstring, commandType, commandText, sqlParameters, timeout);
        }
        #endregion

        #region ExecutePageTable

        /// <summary>
        /// 分页函数
        /// </summary>
        /// <param name="sql">Sql语句</param>
        /// <param name="order">排序字段：ID ASC,Name DESC</param>
        /// <param name="pageIndex">分页索引</param>
        /// <param name="pageSize">分页方法</param>
        /// <param name="records">总记录数</param>
        /// <returns></returns>
        protected DataTable ExecutePageTable(string sql, string order, int pageIndex, int pageSize, out int records)
        {
            return DbHelper.ExecutePageTable(_connstring, sql, order, pageIndex, pageSize, out records);
        }


        /// <summary>
        /// 分页函数
        /// </summary>
        /// <param name="sql">Sql语句</param>
        /// <param name="sqlParameters">参数</param>
        /// <param name="order">排序字段：ID ASC,Name DESC</param>
        /// <param name="pageIndex">分页索引</param>
        /// <param name="pageSize">分页方法</param>
        /// <param name="records">总记录数</param>
        /// <returns></returns>
        protected DataTable ExecutePageTable(string sql, SqlParameter[] sqlParameters, string order, int pageIndex, int pageSize, out int records)
        {
            return ExecutePageTable(_connstring, sql, sqlParameters, order, pageIndex, pageSize, out records);
        }

        /// <summary>
        /// 分页函数
        /// </summary>
        /// <param name="sql">Sql语句</param>
        /// <param name="sqlParameters">参数</param>
        /// <param name="order">排序字段：ID ASC,Name DESC</param>
        /// <param name="pageIndex">分页索引</param>
        /// <param name="pageSize">分页方法</param>
        /// <param name="records">总记录数</param>
        /// <param name="timeout">超时时间</param>
        /// <returns></returns>
        protected DataTable ExecutePageTable(string sql, SqlParameter[] sqlParameters, string order, int pageIndex, int pageSize, out int records, int timeout)
        {
            return DbHelper.ExecutePageTable(_connstring, sql, sqlParameters, order, pageIndex, pageSize, out records, timeout);
        }

        #endregion

        #region Transaction

        /// <summary>
        /// 开始事务，请务必调用TransCommit()或者TransRollback()方法来释放连接。
        /// 只对ExecuteNonQuery/ExecuteScalar方法有效
        /// </summary>
        /// <param name="isExceptionAutoRollback">异常时是否自动回滚事务，建议true</param>
        public void TransBegin(bool isExceptionAutoRollback = true)
        {
            _isTrans = true;
            _isExceptionAutoRollback = isExceptionAutoRollback;
            if (_sqlConnection == null)
            {
                _sqlConnection = new SqlConnection(_connstring);
            }
            if (_sqlConnection.State != ConnectionState.Open)
            {
                _sqlConnection.Open();
            }
            _sqlTrans = _sqlConnection.BeginTransaction();
        }

        /// <summary>
        /// 提交事务并释放连接
        /// </summary>
        public void TransCommit()
        {
            if (_sqlTrans != null)
            {
                _sqlTrans.Commit();
            }
            if (_sqlConnection != null)
            {
                if (_sqlConnection.State == ConnectionState.Open)
                {
                    _sqlConnection.Close();
                }
                _sqlConnection.Dispose();
                _sqlConnection = null;
            }
            _isTrans = false;
        }

        /// <summary>
        /// 回滚事务并释放连接
        /// </summary>
        public void TransRollback()
        {
            if (_sqlTrans != null)
            {
                _sqlTrans.Rollback();
                _sqlTrans = null;
            }
            if (_sqlConnection != null)
            {
                if (_sqlConnection.State == ConnectionState.Open)
                {
                    _sqlConnection.Close();
                }
                _sqlConnection.Dispose();
                _sqlConnection = null;
            }
            _isTrans = false;
        }

        #endregion

        #endregion __实例方法__

        #region __保护方法__
        /// <summary>
        /// 代理操作
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="cmd"></param>
        /// <returns></returns>
        protected delegate T _ExecuteCommand<T>(SqlCommand cmd);
        /// <summary>
        /// 执行数据库操作方法
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="commandType"></param>
        /// <param name="commandText"></param>
        /// <param name="sqlParameters"></param>
        /// <param name="timeout"></param>
        /// <param name="execute"></param>
        /// <returns></returns>
        protected T _RunTransCommand<T>(CommandType commandType, string commandText, SqlParameter[] sqlParameters, int timeout, _ExecuteCommand<T> execute)
        {
            try
            {
                SqlCommand sqlCmd = new SqlCommand();
                sqlCmd.CommandText = commandText;
                sqlCmd.CommandType = commandType;
                sqlCmd.Connection = _sqlConnection;
                sqlCmd.Transaction = _sqlTrans;
                if (timeout > 0)
                {
                    sqlCmd.CommandTimeout = timeout;
                }
                if (sqlParameters != null)
                {
                    foreach (SqlParameter p in sqlParameters)
                    {
                        if (p != null)
                        {
                            if ((p.Direction == ParameterDirection.InputOutput ||
                                p.Direction == ParameterDirection.Input) &&
                                (p.Value == null))
                            {
                                p.Value = DBNull.Value;
                            }
                            sqlCmd.Parameters.Add(p);
                        }
                    }
                }
                return execute(sqlCmd);
            }
            catch (Exception ex)
            {
                if (_isTrans && _isExceptionAutoRollback)
                {
                    this.TransRollback();
                }
                throw ex;
            }
        }
        #endregion __保护方法__
    }
}
