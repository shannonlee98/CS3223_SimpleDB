package test;

import simpledb.controller.Setting;
import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.*;
import simpledb.server.SimpleDB;

import java.util.Scanner;

import simpledb.record.Schema;

import java.sql.Types;

public class SimpleIJ {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.println("Connect> ");
        // String s = sc.nextLine();

        try {
            // analogous to the driver
            SimpleDB db = new SimpleDB("studentdb");

            // analogous to the connection
            Transaction tx = db.newTx();
            Planner planner = db.planner();

            System.out.print("\nSQL> ");
            while (sc.hasNextLine()) {
                // process one line of input
                String cmd = sc.nextLine().trim();
                if (cmd.startsWith("exit"))
                    break;
                else if (cmd.startsWith("select"))
                    doQuery(planner, tx, cmd);
                else if (cmd.startsWith("setting")) {
                    doSetting(planner, cmd);
                } else
                    doUpdate(planner, tx, cmd);
                System.out.print("\nSQL> ");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        sc.close();
    }

    private static void doQuery(Planner planner, Transaction tx, String cmd) {
        try {
            Setting.getInstance().setTimeStart();
            Plan p = planner.createQueryPlan(cmd, tx);

            // analogous to the result set
            Scan s = p.open();

            Schema sch = p.schema();

            int numcols = sch.fields().size();
            int totalwidth = 0;

            if (Setting.getInstance().isPrintResults()) {

                // print header
                for (int i = 1; i <= numcols; i++) {
                    String fldname = sch.fields().get(i - 1);
                    int width = getColumnDisplaySize(fldname, sch);
                    totalwidth += width;
                    String fmt = "%" + width + "s";
                    System.out.format(fmt, fldname);
                }
                System.out.println();
                for (int i = 0; i < totalwidth; i++)
                    System.out.print("-");
                System.out.println();

            }

            // print records
            while (s.next()) {
                if (Setting.getInstance().isPrintResults()) {
                    for (int i = 1; i <= numcols; i++) {
                        String fldname = sch.fields().get(i - 1);
                        int fldtype = sch.type(fldname);
                        String fmt = "%" + getColumnDisplaySize(fldname, sch);
                        if (fldtype == Types.INTEGER) {
                            int ival = s.getInt(fldname);
                            System.out.format(fmt + "d", ival);
                        } else {
                            String sval = s.getString(fldname);
                            System.out.format(fmt + "s", sval);
                        }
                    }
                    System.out.println();
                }
            }
            tx.commit();

            Setting.getInstance().setTimeStop();
            Setting.getInstance().printTimeElapsed();
        } catch (Exception e) {
            System.out.println("SQL Exception: " + e.getMessage());
        }
    }

    private static int getColumnDisplaySize(String fldname, Schema sch) {
        int fldtype = sch.type(fldname);
        int fldlength = (fldtype == Types.INTEGER) ? 6 : sch.length(fldname);
        return Math.max(fldname.length(), fldlength) + 1;
    }

    private static void doUpdate(Planner planner, Transaction tx, String cmd) {
        try {
            int howmany = planner.executeUpdate(cmd, tx);
            System.out.println(howmany + " records processed");
        } catch (Exception e) {
            System.out.println("SQL Exception: " + e.getMessage());
        }
    }

    private static void doSetting(Planner planner, String cmd) {
        try {
            planner.executeSetting(cmd);
            System.out.println("[success]");
        } catch (Exception e) {
            System.out.println("SQL Exception: " + e.getMessage());
            System.out.println("[unsuccessful]");
        }
    }
}