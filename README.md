
LingDber is a lightweight, Open Source(LGPL), database helper library for ASP.NET(C#), .

## Features

* Single table to insert, delete, update and query,  don't need to to write Sql code. 
* Provide database method for complex Sql.
* Provide database method for paging.
* Free transfer DataTable, DbModel or entity class.
* Support for transaction.




## Usage


1.Create dber class and map the database table.  

```C#
/// <summary>
/// Create UserDber and map the database table T_User
/// </summary>
public class UserDber : Ling.Dber.DbTable
{
	public UserDber()
	{
		//Initialize the table and database connection string
		Setting("T_User", System.Configuration.ConfigurationManager.ConnectionStrings["connstring"].ConnectionString);
	}

	//other method for complex Sql.
}
```

2.Instantiate the class UserDber and user.

```C#

//Instantiate UserDber
UserDber dber = new UserDber();


//******* DataTable *********/
//select all records and return DataTable
DataTable dtList = dber.DataTable();
foreach(DataRow row in dtList)
{
	//do something
}


//******* DbModel *********/
//select UserID less than 10000 and return DbModel

List<Ling.Dber.DbModel> dmList = dber.Where("UserID<10000").List();
foreach(DbModel dm in dmList)
{
	//do something
	string username = dm["UserName"];
	//or use
	//string username = dm.GetString("UserName");
}


//******* Class *********/
//select UserID less than 10000 with sql paramters, and  and return UserModel

//UserModel define
public class UserModel
{
	public int UserID{set;get;}
	public string UserName{set;get;}
}

DbModel sqlParamMdel = DbModel();	//create SqlParamters
sqlParamMdel["MaxUserID"] = 10000;	//Assign a value

List<UserModel> umList = dber.Where("UserID<@MaxUserID", sqlParamMdel).List<UserModel>();
foreach(UserModel um in umList)
{
	//do something
	string username = um.UserName;
}


```

## Download

https://github.com/laufin/ling.dber  

## Author

Author: laufin  

QQ: 250047953  

Email: laufin(at)qq.com  

Github: https://github.com/laufin  

Weibo: http://weibo.com/laufin



## Copyright

Licensed:  LGPL-2.1 <http://opensource.org/licenses/lgpl-2.1.php>  

Copyright (c) 2012-2013 Laufin,all rights reserved.  





