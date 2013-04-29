// Github: http://www.github.com/laufin
// Licensed: LGPL-2.1 <http://opensource.org/licenses/lgpl-2.1.php>
// Author: Laufin <laufin@qq.com>
// Copyright (c) 2012-2013 Laufin,all rights reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Web;

namespace Ling.Dber
{

    /// <summary>
    /// 数据实体类
    /// Data model class
    /// </summary>
    public class DbModel
    {
        /// <summary>
        /// 表名
        /// </summary>
        protected string TableName = string.Empty;

        /// <summary>
        /// 数据存储
        /// </summary>
        Dictionary<string, object> _attrList = new Dictionary<string, object>();

        /// <summary>
        /// 构造函数
        /// </summary>
        public DbModel()
        {
        }

        /// <summary>
        /// 读写值
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public object this[string key]
        {
            set
            {
                if (_attrList.ContainsKey(key))
                {
                    _attrList[key] = value;
                }
                else
                {
                    _attrList.Add(key, value);
                }
            }
            get
            {
                if (!_attrList.ContainsKey(key))
                {
                    throw new Exception(string.Format("不存在{0}的属性", key));
                }
                return _attrList[key];
            }
        }

        /// <summary>
        /// 读写值
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public object this[int index]
        {
            set
            {
                string key = _attrList.Keys.ElementAt<string>(index);
                if (_attrList.ContainsKey(key))
                {
                    _attrList[key] = value;
                }
                else
                {
                    _attrList.Add(key, value);
                }
            }
            get
            {
                string key = _attrList.Keys.ElementAt<string>(index);
                if (!_attrList.ContainsKey(key))
                {
                    throw new Exception(string.Format("不存在{0}的属性", key));
                }
                return _attrList[key];
            }
        }

        /// <summary>
        /// 判断是否存在
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool ContainsKey(string key)
        {
            return _attrList.ContainsKey(key);
        }

        /// <summary>
        /// 取整型
        /// </summary>
        /// <param name="key"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public int GetInt(string key, int defaultValue = 0)
        {
            object objValue = this[key];
            try
            {
                if (objValue != null && objValue.ToString() != "")
                {
                    return Convert.ToInt32(objValue);
                }
            }
            catch { }
            return defaultValue;
        }

        /// <summary>
        /// 取长整型
        /// </summary>
        /// <param name="key"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public long GetLong(string key, long defaultValue = 0)
        {
            object objValue = this[key];
            try
            {
                if (objValue != null && objValue.ToString() != "")
                {
                    return Convert.ToInt64(objValue);
                }
            }
            catch { }
            return defaultValue;
        }

        /// <summary>
        /// 取浮点型
        /// </summary>
        /// <param name="key"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public float GetFloat(string key, float defaultValue = 0)
        {
            object objValue = this[key];
            try
            {
                if (objValue != null && objValue.ToString() != "")
                {
                    return Convert.ToSingle(objValue);
                }
            }
            catch { }
            return defaultValue;
        }


        /// <summary>
        /// 取Double值
        /// </summary>
        /// <param name="key"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public double GetDouble(string key, double defaultValue = 0)
        {
            object objValue = this[key];
            try
            {
                if (objValue != null && objValue.ToString() != "")
                {
                    return Convert.ToDouble(objValue);
                }
            }
            catch { }
            return defaultValue;
        }

        /// <summary>
        /// 取字符串
        /// </summary>
        /// <param name="key"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public string GetString(string key, string defaultValue = "")
        {
            object objValue = this[key];
            try
            {
                if (objValue != null)
                {
                    return objValue.ToString();
                }
            }
            catch { }
            return defaultValue;
        }

        /// <summary>
        /// 取decimal值
        /// </summary>
        /// <param name="key"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public decimal GetDecimal(string key, decimal defaultValue = 0)
        {
            object objValue = this[key];
            try
            {
                if (objValue != null && objValue.ToString() != "")
                {
                    return Convert.ToDecimal(objValue);
                }
            }
            catch { }
            return defaultValue;
        }

        /// <summary>
        /// 取DateTime值
        /// </summary>
        /// <param name="key"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public DateTime GetDateTime(string key, DateTime defaultValue)
        {
            object objValue = this[key];
            if (objValue != null && objValue.ToString() != "")
            {
                return Convert.ToDateTime(objValue);
            }
            return defaultValue;
        }


        /// <summary>
        /// 取DateTime值
        /// </summary>
        /// <param name="key"></param>
        /// <param name="format"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public string GetDateTimeString(string key, string format, string defaultValue = "")
        {
            object objValue = this[key];
            try
            {
                if (objValue != null && objValue.ToString() != "")
                {
                    DateTime dt = Convert.ToDateTime(objValue);
                    if (!string.IsNullOrEmpty(format))
                    {
                        return dt.ToString(format);
                    }
                    else
                    {
                        return dt.ToString();
                    }
                }
            }
            catch { }
            return defaultValue;
        }

        /// <summary>
        /// 取参数列表
        /// </summary>
        /// <returns></returns>
        public Dictionary<string, object> GetAttrList()
        {
            return _attrList;
        }


        /// <summary>
        /// 获取整型列表
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public T Get<T>(string key)
        {
            object obj = this[key];
            if (obj != null)
            {
                return (T)obj;
            }
            return default(T);
        }


        /// <summary>
        /// 创建一个实例
        /// </summary>
        /// <returns></returns>
        public static DbModel New()
        {
            return new DbModel();
        }

        /// <summary>
        /// 赋值
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public DbModel Set(string key, object value)
        {
            this[key] = value;
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public T ToModel<T>() where T : class, new ()
        {
            T model = null;
            if (_attrList != null && _attrList.Count > 0)
            {
                model = new T();
                //string cacheKey = "LING_DBER_CACHE_" + model.GetType().FullName;
                //System.Reflection.PropertyInfo[] piList = HttpRuntime.Cache[model.GetType().FullName] as System.Reflection.PropertyInfo[];
                //if (piList == null)
                //{
                //    piList = model.GetType().GetProperties();
                //    HttpRuntime.Cache.Insert(cacheKey, piList, null, DateTime.Now.AddDays(1), TimeSpan.Zero);
                //}
                foreach (System.Reflection.PropertyInfo info in model.GetType().GetProperties())
                {
                    if (this.ContainsKey(info.Name))
                    {
                        info.SetValue(model, this[info.Name], null);
                    }
                }
            }
            return model;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="model"></param>
        public void FromModel<T>(T model) where T : class, new()
        {
            if (model == null) return;
            foreach (System.Reflection.PropertyInfo info in model.GetType().GetProperties())
            {
                this[info.Name] = info.GetValue(model, null);
            }
        }

    }



}
