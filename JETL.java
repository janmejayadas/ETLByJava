package com.sql.etl.test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;

public class JETL {

	public static String[] parseSql(String schema) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(schema)));
		String mysql = "";
		String line;
		while ((line = br.readLine()) != null) {
			mysql = mysql + line;
			if (null != mysql) {
			}
		}
		br.close();
		mysql = mysql.replaceAll("`", "");
		System.out.println(mysql);

		return mysql.split(";");
	}

	//
	public static List<String> changeTableName(String schemaFile, String newTblName, String suffix)
			throws JSQLParserException, IOException {
		CCJSqlParserManager pm = new CCJSqlParserManager();
		List<String> listOfStr = new ArrayList<String>();
		String newTableFormat = null;
		String[] sqlStatements = parseSql(schemaFile);
		// System.out.println("-------------------------------");
		for (String sqlStatement : sqlStatements) {
			Statement statement = pm.parse(new StringReader(sqlStatement));
			if (statement instanceof CreateTable) {
				CreateTable create = (CreateTable) statement;
				// ColumnDefinition newClm=new ColumnDefinition();
				// newClm.setColumnName("devId");
				// newClm.setColumnName("char(12)");
				// List<String> listOfColm=newClm.getColumnSpecStrings();
				// System.out.println("ListColumn="+listOfColm);
				// create.setColumnDefinitions(list);
				create.setTable(new Table(newTblName));
				listOfStr.add(create.toString() + "; \r");
			} else if (statement instanceof Drop) {
				Drop droptable = (Drop) statement;
				droptable.setName(new Table(newTblName + "; \r"));
				listOfStr.add(droptable.toString());
			}
			if (statement instanceof Insert) {
				Insert insert = (Insert) statement;
				String name = insert.getTable().getName();
				if (name.endsWith(suffix)) {
					insert.setTable(new Table(newTblName));
				}
				listOfStr.add(insert.toString() + "; \r");
			}
		}
		return listOfStr;
	}

	public static void main(String[] args) throws Exception {

		// String schemaFile =
		// "C:\\Users\\jandas\\eclipse-workspace\\ETL_zql\\src\\1065a302_events.sql";
		// String tableName = "_1065a3020000_events";
		// String newTablName = "bb232_events";
		// String suffix = "_events";

		Properties props = new Properties();
		props.load(JETL.class.getClassLoader().getResourceAsStream("config.properties"));
		String schemaFile = props.getProperty("oldschemaFile");
		String newTablName = props.getProperty("newTablName");
		String suffix = props.getProperty("suffix");
		String newSchemaFile = props.getProperty("newschemaFile");
		String makeFile = newSchemaFile + newTablName + ".sql";

		System.out.println("start...");

		// List<String> columnNames = ApiPoc.getColumnNames(tableName, schemaFile);
		// System.out.println(columnNames);
		// List<String> dataTypes = BUDataColumnsFinder.getType(tableName, schemaFile);

		List<String> listOfStr = JETL.changeTableName(schemaFile, newTablName, suffix);

		System.out.println("After..");
		System.out.println(listOfStr);

		FileWriter fw = new FileWriter(makeFile);
		boolean drpFlag = false;
		boolean crtFlag = false;
		String drop = "";
		String creat = "";
		boolean mismatch = false;
		for (int i = 0; i < listOfStr.size(); i++) {
			for (int j = i + 1; j < listOfStr.size(); j++) {
				if ((listOfStr.get(i).startsWith("DROP")) && (listOfStr.get(j).startsWith("DROP"))) {
					if (listOfStr.get(i).equals(listOfStr.get(j))) {
						drpFlag = true;
						drop = listOfStr.get(i);
					}
				} else if ((listOfStr.get(i).startsWith("CREATE")) && (listOfStr.get(j).startsWith("CREATE"))) {
					if (listOfStr.get(i).equals(listOfStr.get(j))) {
						crtFlag = true;
						creat = listOfStr.get(i);
					} else {
						System.out.println("TableMismatch !!!!!");
						System.out.println(listOfStr.get(i) + "--" + listOfStr.get(j));
						mismatch = true;
						break;
					}
				}
				if (mismatch) {
					break;
				}
			}
			if (mismatch) {
				break;
			}
		}

		if (drpFlag && crtFlag && mismatch != true) {
			fw.write(drop);
			String mysql = "";
			if (crtFlag) {
				mysql = creat;
				if (null != mysql) {
					mysql = mysql.replace("_status (", "_status ( " + "devId char(12) NOT NULL,");
					mysql = mysql.replace(") ENGINE", " ,KEY devId (devId)\r" + ") ENGINE");
				}
			}
			fw.write(mysql);
		}
		int count = 0;
		for (int i = 0; i < listOfStr.size(); i++) {
			if ((listOfStr.get(i).startsWith("INSERT INTO"))) {
				String mysql = "";
				count++;
				if (listOfStr.get(i) != null) {
					mysql = listOfStr.get(i);
					if (null != mysql) {
						mysql = mysql.replace("VALUES (1,", "VALUES (" + String.valueOf(count) + ",");

						mysql = mysql.replace("VALUES (", "VALUES ('BB-RS 232', ");

					}
				}

				fw.write(mysql);
			}
		}
		fw.flush();
		fw.close();
	}
}
