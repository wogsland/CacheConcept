/*
 * The MIT License
 *
 * Copyright 2014 Bradley J. Wogsland.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package CacheConcept;

import java.sql.*;

/**
 * MyQuery - MySQL query ORM to return a cached query where possible and
 * otherwise hit the DB, caching the result. Assumes DB has a cache table
 * of the form:
 * 
 * CREATE TABLE `mysql_cache` (
 * `id` int(11) NOT NULL AUTO_INCREMENT,
 * `invalid` int(1),
 * `tables` varchar(1000) NOT NULL,
 * `where_clauses` varchar(4000) NOT NULL,
 * `group_bys` varchar(1000) NOT NULL,
 * `results` varchar(1000) NOT NULL, 
 * PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB AUTO_INCREMENT=437 DEFAULT CHARSET=latin1;
 * 
 * @author Bradley J. Wogsland
 * @version 0.1.20140503
 */
public class MyQuery {
    
    // Database info
    private static String mydb_name = "";
    private static String mydb_database = "";
    private static String mydb_user = "";
    private static String mydb_password = "";

    // Query info
    private String[] select = new String[0];
    private String selectJSON = "{}";
    private String[] tables = new String[0];
    private String tablesJSON = "{}";
    private String[] where_clauses = new String[0];
    private String where_clausesJSON = "{}";
    private String[] group_bys = new String[0];
    private String group_bysJSON = "{}";
    private String resultsJSON = "";
    
    public String full(){
        String returnquery = "select ";
        for(int i=0;i<select.length;i++){
            returnquery = returnquery + select[i];
            if(i != select.length - 1){
                returnquery = returnquery + ", ";
            }
        }
        returnquery = returnquery + " from ";
        for(int i=0;i<tables.length;i++){
            returnquery = returnquery + tables[i];
            if(i != tables.length - 1){
                returnquery = returnquery + ", ";
            }
        }
        if(where_clauses.length > 0){
            returnquery = returnquery + " where ";
            for(int i=0;i<where_clauses.length;i++){
                returnquery = returnquery + where_clauses[i];
                if(i != where_clauses.length - 1){
                    returnquery = returnquery + " and ";
                }
            }            
        }
        if(group_bys.length > 0){
            returnquery = returnquery + " group by ";
            for(int i=0;i<group_bys.length;i++){
                returnquery = returnquery + group_bys[i];
                if(i != group_bys.length - 1){
                    returnquery = returnquery + ", ";
                }
            }            
        }
        
        return returnquery;
    }//full
    
    public void setSelect(String[] my_selects){
        select = my_selects;
    }//setSelect
    
    public String[] getSelect(){
        return select;
    }//setSelect
    
    public void setTables(String[] my_tables){
        tables = my_tables;
        tablesJSON = "{";
        for(int i=0;i<my_tables.length;i++){
            tablesJSON = tablesJSON + "{\"table\":\"" + my_tables[i] + "\"}";
            if(i != my_tables.length-1){
                tablesJSON = tablesJSON + ",";
            }
        }
        tablesJSON = tablesJSON + "}";
    }//setTables
    
    public void setWheres(String[] my_wheres){
        where_clauses = my_wheres;
        where_clausesJSON = "{";
        for(int i=0;i<my_wheres.length;i++){
            where_clausesJSON = where_clausesJSON + "{\"where\":\"" + my_wheres[i] + "\"}";
            if(i != my_wheres.length-1){
                where_clausesJSON = where_clausesJSON + ",";
            }
        }
        where_clausesJSON = where_clausesJSON + "}";
    }//setWheres
    
    public void setGroupBys(String[] my_groupbys){
        group_bys = my_groupbys;
        group_bysJSON = "{";
        for(int i=0;i<my_groupbys.length;i++){
            group_bysJSON = group_bysJSON + "{\"group by\":\"" + my_groupbys[i] + "\"}";
            if(i != my_groupbys.length-1){
                group_bysJSON = group_bysJSON + ",";
            }
        }
        group_bysJSON = group_bysJSON + "}";
    }//setGroupBys
    
    /* 
     * getResults - this gets the query's result set in JSON format from one
     * of the following places:
     *      (1). This class (if it's not empty).
     *      (2). The cache table in the MySQL database.
     *      (3). Running the query on the MySQL database.
     */
    public String getResults(){
        if(resultsJSON != ""){
            System.out.println("Results (Class Variable):");
            return resultsJSON;
        }else{
            
            //We're gonna need a database connection
            Connection conn = connectDB();
                    
            //look in the cache table
            MyQuery q = new MyQuery();
            q.setSelect(new String[]{"results"});
            q.setTables(new String[]{"mysql_cache"});        
            q.setWheres(new String[]{"invalid = 0",
                                     "tables = '" + tablesJSON + "'",
                                     "where_clauses = '" + where_clausesJSON + "'",
                                     "group_bys = '" + group_bysJSON + "'",
                                    });        
            Statement stmt = null;
            ResultSet rs = null;                     
            try {
                stmt = conn.createStatement();
                rs = stmt.executeQuery(q.full());
                if(rs.first()){
                    System.out.println("Results (Cached):");
                    resultsJSON = rs.getString("results");
                }else{
                    //It's not cached yet, query the database.
                    stmt = null;
                    rs = null;                     
                    try {
                        stmt = conn.createStatement();
                        rs = stmt.executeQuery(full());

                        // Print the ResultSet.
                        rs.first();
                        String[] my_selects = getSelect();
                        String[] results = new String[my_selects.length];
                        String result_json = "{";
                        while(!rs.isAfterLast()){
                            result_json = result_json + "{";
                            for(int i=0;i<my_selects.length;i++){
                                results[i] = rs.getString(my_selects[i]);
                                result_json = result_json + "\"" + my_selects[i] + "\":\"" + results[i] + "\"";
                                if(i != my_selects.length - 1){
                                    result_json = result_json + ", ";
                                }else{
                                    result_json = result_json + "}";
                                }
                            }
                            result_json = result_json + "}";
                            if(!rs.isLast()){
                                result_json = result_json + ",";
                            }
                            rs.next();
                        }
                        System.out.println("Results (Querying DB):");
                        resultsJSON = result_json;
                    }catch (SQLException ex){
                        // handle any errors
                        System.out.println("SQLException: " + ex.getMessage());
                        System.out.println("SQLState: " + ex.getSQLState());
                        System.out.println("VendorError: " + ex.getErrorCode());
                        resultsJSON = "crap";
                    }
                    //If query was a success, cache it.
                    if(resultsJSON != "crap"){
                        try {
                            Statement instmt = conn.createStatement();
                            String new_results = "insert into mysql_cache "
                                                        + "(invalid,"
                                                        + "tables,"
                                                        + "where_clauses,"
                                                        + "group_bys,"
                                                        + "results)"
                                                + " values (0,"
                                                        + "'" + tablesJSON + "',"
                                                        + "'" + where_clausesJSON + "',"
                                                        + "'" + group_bysJSON + "',"
                                                        + "'" + resultsJSON + "')";
                            //System.out.println(new_results);
                            instmt.executeUpdate(new_results);
                        }catch (SQLException ex){
                            // handle any errors
                            System.out.println("SQLException: " + ex.getMessage());
                            System.out.println("SQLState: " + ex.getSQLState());
                            System.out.println("VendorError: " + ex.getErrorCode());
                        }
                    }else{
                        resultsJSON = "";
                    }
                }
                
                //cleanup
                rs.close();
                stmt.close();
                conn.close();
                        
            }catch (SQLException ex){
                // handle any errors
                System.out.println("SQLException: " + ex.getMessage());
                System.out.println("SQLState: " + ex.getSQLState());
                System.out.println("VendorError: " + ex.getErrorCode());
                resultsJSON = "crap";
            }
            
            
            return resultsJSON;
        }
    }//getResults
    
    private Connection connectDB(){
        
        //Load in the MySQL driver.
        LoadDriver.main();
        
        //Connect to the DB
        System.out.println("Attempting to connect to the DB...\r\n");
        Connection conn = null;
               
        // Do it!
        try {
            conn =
               DriverManager.getConnection("jdbc:mysql://" + mydb_name + 
                                           "/" + mydb_database +
                                           "?" + "user=" + mydb_user + 
                                           "&password=" + mydb_password);
            return conn;
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
            return null;
        }
        
    }//connectDB
    
}//MyQuery
