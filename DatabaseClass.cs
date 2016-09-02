using System;
using System.Collections.Generic;
using System.Reflection;
using System.Data;
using System.Data.SqlClient;

namespace DataLibrary
{
    public partial class DatabaseClass
    {
        private static DatabaseClass _Database;
        internal SqlConnection ActiveSQLConn;
        private bool _ConnectionOpen = false;
        private string _ConnectionString
        {
            get
            {
                String EnvironmentName = System.Configuration.ConfigurationManager.AppSettings["AppEnvironment"] ?? "Local";

                switch (EnvironmentName)
                {
                    case "Local":
                        return Properties.Settings.Default.Local_SQLConnection;
                    case "Development":
                        return Properties.Settings.Default.Utility_Development_SQLConnection;
                    case "Preview":
                        return Properties.Settings.Default.Utility_Preview_SQLConnection;
                    case "Production":
                        return Properties.Settings.Default.Utility_Production_SQLConnection;
                    default:
                        return Properties.Settings.Default.Local_SQLConnection;
                }
            }
        }

        public SqlTransaction CurrentTransaction;

        public static DatabaseClass GetInstance()
        {
            try
            {
                DatabaseClass oDatabase = null;
                if (System.Web.HttpContext.Current != null)
                {

                    if (!System.Web.HttpContext.Current.Items.Contains("ClassLibrary2_DB"))
                    {
                        oDatabase = new DatabaseClass();
                        System.Web.HttpContext.Current.Items.Add("ClassLibrary2_DB", oDatabase);
                    }
                    else
                    {
                        oDatabase = (DatabaseClass)System.Web.HttpContext.Current.Items["ClassLibrary2_DB"];
                    }
                }
                else
                {
                    if (_Database == null)
                        _Database = new DatabaseClass();
                    oDatabase = _Database;
                }

                return oDatabase;
            }

            catch (Exception ex)
            {
                throw new Exception(ex.Message, ex.InnerException);
            }
        }

        public DataSet Select(string SQL, SqlParameter[] SQLParameters = null)
        {
            bool bOpened = false;
            try
            {
                bOpened = OpenDB();

                SqlDataAdapter oAdapter = new SqlDataAdapter(SQL, ActiveSQLConn);
                DataSet oDS = new DataSet();

                oAdapter.SelectCommand.CommandType = CommandType.TableDirect;
                oAdapter.SelectCommand.Transaction = CurrentTransaction;

                if (SQLParameters != null)
                {
                    foreach (SqlParameter oSQLParameter in SQLParameters)
                    {
                        oAdapter.SelectCommand.Parameters.Add(oSQLParameter);
                    }
                }

                //oAdapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;
                oAdapter.Fill(oDS);
                oAdapter.SelectCommand.Parameters.Clear();
                return oDS;
            }

            catch (Exception ex)
            {
                if (ex.Source.IndexOf("::") == -1)
                    ex.Source = GetType().Name + "::" + MethodBase.GetCurrentMethod().Name;
                throw new Exception(ex.Message, ex.InnerException);
            }
            finally
            {
                if (bOpened == true)
                    CloseDB();
            }
        }
        
        public DataSet getDataset(string ViewName, string SelectColumns, string Filter)
        {
            bool bOpened = false;
            try
            {
                bOpened = OpenDB();

                string SQL = SelectColumns + " " + ViewName + " " + Filter + " ";

                SqlDataAdapter oAdapter = new SqlDataAdapter(SQL, ActiveSQLConn);
                DataSet oDS = new DataSet();

                oAdapter.SelectCommand.CommandType = CommandType.Text;
                oAdapter.SelectCommand.Transaction = CurrentTransaction;

                oAdapter.Fill(oDS);

                return oDS;
            }

            catch (Exception ex)
            {
                if (ex.Source.IndexOf("::") == -1)
                    ex.Source = GetType().Name + "::" + MethodBase.GetCurrentMethod().Name;
                throw new Exception(ex.Message, ex.InnerException);
            }
            finally
            {
                if (bOpened == true)
                    CloseDB();
            }
        }

        public DataSet getDataset(object ViewClass, Boolean SelectDistinct, int Limit = 0)
        {
            bool bOpened = false;
            try
            {
                bOpened = OpenDB();

                System.Type oType = ViewClass.GetType();
                PropertyInfo pColumnsInfo = oType.GetProperty("Columns");
                VWColumn oColumns = (VWColumn)pColumnsInfo.GetValue(ViewClass, null);

                PropertyInfo pFilterInfo = oType.GetProperty("Filter");
                VWFilter oFilter = (VWFilter)pFilterInfo.GetValue(ViewClass, null);

                PropertyInfo pViewInfo = oType.GetProperty("ViewName");
                string ViewName = pViewInfo.GetValue(ViewClass, null).ToString();

                PropertyInfo pOrderbyInfo = oType.GetProperty("OrderBy");
                VWOrderBy Orderby = (VWOrderBy)pOrderbyInfo.GetValue(ViewClass, null);

                PropertyInfo pGroupbyInfo = oType.GetProperty("GroupBy");
                VWGroupBy Groupby = (VWGroupBy)pGroupbyInfo.GetValue(ViewClass, null);

                string SQL = oColumns.GetColumns(SelectDistinct, Limit) + " " + "from " + ViewName + " " + oFilter.getFilter() + " " + Groupby.GetColumns() + " " + Orderby.GetColumns() + " ";

                SqlDataAdapter oAdapter = new SqlDataAdapter(SQL, ActiveSQLConn);
                DataSet oDS = new DataSet();

                oAdapter.SelectCommand.CommandType = CommandType.Text;
                oAdapter.SelectCommand.Transaction = CurrentTransaction;

                //oAdapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;
                oAdapter.Fill(oDS);

                return oDS;
            }

            catch (Exception ex)
            {
                if (ex.Source.IndexOf("::") == -1)
                    ex.Source = GetType().Name + "::" + MethodBase.GetCurrentMethod().Name;
                throw new Exception(ex.Message, ex.InnerException);
            }
            finally
            {
                if (bOpened == true)
                    CloseDB();
            }
        }

        public int ExecStoredProcedure(string ProcName)
        {
            bool bOpened = false;
            try
            {
                bOpened = OpenDB();

                SqlCommand oCMD = new SqlCommand(ProcName, ActiveSQLConn, CurrentTransaction);
                oCMD.CommandType = CommandType.StoredProcedure;
                return oCMD.ExecuteNonQuery();
            }

            catch (Exception ex)
            {
                if (ex.Source.IndexOf("::") == -1)
                    ex.Source = GetType().Name + "::" + MethodBase.GetCurrentMethod().Name;
                throw new Exception(ex.Message, ex.InnerException);
            }
            finally
            {
                if (bOpened == true)
                    CloseDB();
            }
        }

        public int ExecStoredProcedure(string ProcName, ref SqlParameter[] SQLParameters)
        {
            bool bOpened = false;
            try
            {
                bOpened = OpenDB();

                SqlCommand oCMD = new SqlCommand(ProcName, ActiveSQLConn, CurrentTransaction);
                oCMD.CommandType = CommandType.StoredProcedure;
                if (SQLParameters != null)
                {
                    oCMD.Parameters.AddRange(SQLParameters);
                }

                int nReturn = oCMD.ExecuteNonQuery();
                oCMD.Parameters.Clear();
                return nReturn;

            }

            catch (Exception ex)
            {
                if (ex.Source.IndexOf("::") == -1)
                    ex.Source = GetType().Name + "::" + MethodBase.GetCurrentMethod().Name;
                throw new Exception(ex.Message, ex.InnerException);
            }
            finally
            {
                if (bOpened == true)
                    CloseDB();
            }
        }

        public int ExecStoredProcedure(string ProcName, ref SqlParameter oSQLParameter)
        {
            bool bOpened = false;
            try
            {
                bOpened = OpenDB();

                SqlCommand oCMD = new SqlCommand(ProcName, ActiveSQLConn, CurrentTransaction);
                oCMD.CommandType = CommandType.StoredProcedure;
                if (oSQLParameter != null)
                {
                    oCMD.Parameters.Add(oSQLParameter);
                }

                int nReturn = oCMD.ExecuteNonQuery();
                oCMD.Parameters.Clear();
                return nReturn;

            }

            catch (Exception ex)
            {
                if (ex.Source.IndexOf("::") == -1)
                    ex.Source = GetType().Name + "::" + MethodBase.GetCurrentMethod().Name;
                throw new Exception(ex.Message, ex.InnerException);
            }
            finally
            {
                if (bOpened == true)
                    CloseDB();
            }
        }

        public object ExecStoredProcedureScaler(string ProcName, ref SqlParameter[] SQLParameters)
        {
            bool bOpened = false;
            try
            {
                bOpened = OpenDB();

                SqlCommand oCMD = new SqlCommand(ProcName, ActiveSQLConn, CurrentTransaction);
                oCMD.CommandType = CommandType.StoredProcedure;
                if (SQLParameters != null)
                {
                    oCMD.Parameters.AddRange(SQLParameters);
                }
                return oCMD.ExecuteScalar();
            }
            catch (Exception ex)
            {
                if (ex.Source.IndexOf("::") == -1)
                    ex.Source = GetType().Name + "::" + MethodBase.GetCurrentMethod().Name;
                throw new Exception(ex.Message, ex.InnerException);
            }
            finally
            {
                if (bOpened == true)
                    CloseDB();
            }
        }

        #region Public Methods

        public bool OpenDB()
        {
            try
            {
                if (ActiveSQLConn == null)
                    ActiveSQLConn = new SqlConnection();

                if (ActiveSQLConn.State == ConnectionState.Closed)
                {
                    ActiveSQLConn.ConnectionString = getConnectionString();
                    ActiveSQLConn.Open();
                    return true;
                }
                else
                {
                    return false;
                }
            }

            catch (Exception ex)
            {
                throw new Exception(ex.Message, ex.InnerException);
            }
        }

        public void CloseDB()
        {
            if (ActiveSQLConn != null && ActiveSQLConn.State == ConnectionState.Open)
                ActiveSQLConn.Close();
        }

        public bool BeginTrans()
        {
            try
            {
                _ConnectionOpen = OpenDB();

                if (CurrentTransaction == null)
                {
                    CurrentTransaction = ActiveSQLConn.BeginTransaction();
                    return true;
                }

                return false;
            }

            catch (Exception ex)
            {
                throw new Exception(ex.Message, ex.InnerException);
            }
        }

        public bool CommitTrans()
        {
            try
            {
                if (CurrentTransaction != null)
                {
                    CurrentTransaction.Commit();
                    CurrentTransaction = null;
                    if (_ConnectionOpen == true)
                    {
                        CloseDB();
                    }
                    return false;
                }
                else
                {
                    if (_ConnectionOpen == true)
                    {
                        CloseDB();
                    }
                    return true;
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message, ex.InnerException);
            }
        }

        public bool RollbackTrans()
        {
            try
            {
                if (CurrentTransaction != null)
                {
                    CurrentTransaction.Rollback();
                    CurrentTransaction = null;
                    if (_ConnectionOpen == true)
                        CloseDB();
                    return false;
                }
                else
                {
                    if (_ConnectionOpen == true)
                        CloseDB();
                    return true;
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message, ex.InnerException);
            }
        }

        public static void RunInTransaction(Action action)
        {
            Boolean bOpened = false;
            Boolean bInTrans = false;

            try
            {
                bOpened = GetInstance().OpenDB();
                bInTrans = GetInstance().BeginTrans();

                action.Invoke();

                if (bInTrans)
                {
                    bInTrans = GetInstance().CommitTrans();
                }
            }
            catch (Exception)
            {
                if (bInTrans)
                {
                    GetInstance().RollbackTrans();
                }

                throw;
            }
            finally
            {
                if (bOpened)
                {
                    GetInstance().CloseDB();
                }
            }
        }

        #endregion

    }
}
