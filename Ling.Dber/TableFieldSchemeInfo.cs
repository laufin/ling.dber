// Github: http://www.github.com/laufin
// Licensed: LGPL-2.1 <http://opensource.org/licenses/lgpl-2.1.php>
// Author: Laufin <laufin@qq.com>
// Copyright (c) 2012-2013 Laufin,all rights reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace Ling.Dber
{
    /// <summary>
    /// 表结构信息
    /// Database table field scheme info
    /// </summary>
    internal class TableFieldSchemeInfo
    {
        /// <summary>
        /// 列名
        /// </summary>
        public string ColumnName { set; get; }

        /// <summary>
        /// 字段类型
        /// </summary>
        public SqlDbType SqlDbType { set; get; }

        /// <summary>
        /// 最大长度
        /// </summary>
        public int MaxSize { set; get; }


        /// <summary>
        /// 创建参数
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public SqlParameter CreateSqlParamter(object value)
        {
            SqlParameter sqlParameter = new SqlParameter();
            sqlParameter.ParameterName = "@" + this.ColumnName;
            sqlParameter.SqlDbType = this.SqlDbType;
            if (MaxSize > 0)
            {
                sqlParameter.Size = MaxSize;
            }
            sqlParameter.Value = value;
            return sqlParameter;
        }

    }
}
