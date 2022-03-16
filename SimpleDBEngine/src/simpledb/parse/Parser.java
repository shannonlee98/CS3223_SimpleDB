package simpledb.parse;

import java.util.*;

import simpledb.controller.Setting;
import simpledb.query.*;
import simpledb.record.*;
import simpledb.materialize.*;

/**
 * The SimpleDB parser.
 *
 * @author Edward Sciore
 */
public class Parser {
    private Lexer lex;

    public Parser(String s) {
        lex = new Lexer(s);
    }

// Methods for parsing predicates, terms, expressions, constants, and fields

    public String field() {
        return lex.eatId();
    }

    public String aggregate() {
        return lex.eatAggregate();
    }

    public Constant constant() {
        if (lex.matchStringConstant())
            return new Constant(lex.eatStringConstant());
        else
            return new Constant(lex.eatIntConstant());
    }

    public Expression expression() {
        if (lex.matchId())
            return new Expression(field());
        else
            return new Expression(constant());
    }

    public CondOp condOperator() {
        return new CondOp(lex.eatCondOp());
    }

    public Term term() {
        Expression lhs = expression();
        CondOp condOp = condOperator();
        Expression rhs = expression();

        return new Term(lhs, condOp, rhs);
    }

    public Predicate predicate() {
        Predicate pred = new Predicate(term());
        if (lex.matchKeyword("and")) {
            lex.eatKeyword("and");
            pred.conjoinWith(predicate());
        }
        return pred;
    }

    //set some stuff in simple ij
    public void setting() {
        lex.eatKeyword("setting");
        Setting.getInstance().set(lex.eatStringConstant());
    }

// Methods for parsing queries

    public QueryData query() {
        lex.eatKeyword("select");
        boolean isDistinct = distinct();
        List<String> fields = new ArrayList<>();
        List<AggregationFn> aggregates = new ArrayList<>();

        while (true) {
            if (lex.matchAggregate()) {
                AggregationFn aggrFn = getAggregateFn();
                aggregates.add(aggrFn);
                fields.add(aggrFn.fieldName());
            } else if (lex.matchDelim('*')) {
                lex.eatDelim('*');
                fields.add("*");
            } else if (lex.matchId())
                fields.add(field());
            else
                throw new BadSyntaxException("Error in select field");

            if (!lex.matchDelim(','))
                break;
            lex.eatDelim(',');
        }

        lex.eatKeyword("from");
        Collection<String> tables = tableList();
        Predicate pred = new Predicate();
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }
        LinkedHashMap<String, Boolean> orderByFields = new LinkedHashMap<>();
        if (lex.matchKeyword("order")) {
            lex.eatKeyword("order");
            lex.eatKeyword("by");
            orderByFields = orderByList();
        }
        List<String> groupByFields = new ArrayList<>();
        if (lex.matchKeyword("group")) {
            lex.eatKeyword("group");
            lex.eatKeyword("by");
            groupByFields = groupByList();
        }
        return new QueryData(isDistinct, fields, aggregates, tables, pred, orderByFields, groupByFields);
    }

    private boolean distinct() {
        if (lex.matchKeyword("distinct")) {
            lex.eatKeyword("distinct");
            return true;
        }
        return false;
    }

    private List<String> selectList() {
        List<String> list = new ArrayList<>();
        if (lex.matchDelim('*')) {
            lex.eatDelim('*');
            list.add("*");
        }
        if (lex.matchId())
            list.add(field());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            list.addAll(selectList());
        }
        return list;
    }


    private AggregationFn getAggregateFn() {
        String aggregate = aggregate();
        boolean isDistinct = false;
        try {
            lex.eatDelim('(');
        } catch (BadSyntaxException e) {
            throw new BadSyntaxException("Delimiter'(' expected in aggregate function");
        }
        if (lex.matchKeyword("distinct")) {
            isDistinct = true;
            lex.eatKeyword("distinct");
        }
        List<String> fields = selectList();
        try {
            lex.eatDelim(')');
        } catch (BadSyntaxException e) {
            throw new BadSyntaxException("Delimiter')' expected in aggregate function");
        }
        if (fields.size() < 1)
            throw new BadSyntaxException("Aggregation function cannot be empty");
        if (fields.size() > 1)
            throw new BadSyntaxException("Too many arguments in aggregation function");

        String field = fields.get(0);
        switch (aggregate.toLowerCase()) {
            case "avg":
                return new AvgFn(field, isDistinct);
            case "count":
                return new CountFn(field, isDistinct);
            case "max":
                return new MaxFn(field, isDistinct);
            case "min":
                return new MinFn(field, isDistinct);
            case "sum":
                return new SumFn(field, isDistinct);
            default:
                throw new BadSyntaxException("Aggregation function not recognised");
        }
    }

    private LinkedHashMap<String, Boolean> orderByList() {
        LinkedHashMap<String, Boolean> orderByFields = new LinkedHashMap<>();
        try {
            String field = field();
            boolean isAsc = true;
            if (lex.matchKeyword("asc")) {
                lex.eatKeyword("asc");
            } else if (lex.matchKeyword("desc")) {
                lex.eatKeyword("desc");
                isAsc = false;
            }

            orderByFields.put(field, isAsc);
        } catch (BadSyntaxException e) {
            throw new BadSyntaxException("Field expected in order by clause");
        }

        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            orderByFields.putAll(orderByList());
        }
        return orderByFields;
    }

    private List<String> groupByList() {
        List<String> groupByFields = new ArrayList<>();
        try {
            String field = field();
            groupByFields.add(field);
        } catch (BadSyntaxException e) {
            throw new BadSyntaxException("Field expected in group by clause");
        }

        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            groupByFields.addAll(groupByList());
        }
        return groupByFields;
    }

    private Collection<String> tableList() {
        Collection<String> L = new ArrayList<>();
        if (lex.matchId())
            L.add(lex.eatId());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(tableList());
        }
        return L;
    }

// Methods for parsing the various update commands

    public Object updateCmd() {
        if (lex.matchKeyword("insert"))
            return insert();
        else if (lex.matchKeyword("delete"))
            return delete();
        else if (lex.matchKeyword("update"))
            return modify();
        else
            return create();
    }

    private Object create() {
        lex.eatKeyword("create");
        if (lex.matchKeyword("table"))
            return createTable();
        else if (lex.matchKeyword("view"))
            return createView();
        else
            return createIndex();
    }

// Method for parsing delete commands

    public DeleteData delete() {
        lex.eatKeyword("delete");
        lex.eatKeyword("from");
        String tblname = lex.eatId();
        Predicate pred = new Predicate();
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }
        return new DeleteData(tblname, pred);
    }

// Methods for parsing insert commands

    public InsertData insert() {
        lex.eatKeyword("insert");
        lex.eatKeyword("into");
        String tblname = lex.eatId();
        lex.eatDelim('(');
        List<String> flds = fieldList();
        lex.eatDelim(')');
        lex.eatKeyword("values");
        lex.eatDelim('(');
        List<Constant> vals = constList();
        lex.eatDelim(')');
        return new InsertData(tblname, flds, vals);
    }

    private List<String> fieldList() {
        List<String> L = new ArrayList<>();
        L.add(field());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(fieldList());
        }
        return L;
    }

    private List<Constant> constList() {
        List<Constant> L = new ArrayList<>();
        L.add(constant());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(constList());
        }
        return L;
    }

// Method for parsing modify commands

    public ModifyData modify() {
        lex.eatKeyword("update");
        String tblname = lex.eatId();
        lex.eatKeyword("set");
        String fldname = field();
        lex.eatDelim('=');
        Expression newval = expression();
        Predicate pred = new Predicate();
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }
        return new ModifyData(tblname, fldname, newval, pred);
    }

// Method for parsing create table commands

    public CreateTableData createTable() {
        lex.eatKeyword("table");
        String tblname = lex.eatId();
        lex.eatDelim('(');
        Schema sch = fieldDefs();
        lex.eatDelim(')');
        return new CreateTableData(tblname, sch);
    }

    private Schema fieldDefs() {
        Schema schema = fieldDef();
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            Schema schema2 = fieldDefs();
            schema.addAll(schema2);
        }
        return schema;
    }

    private Schema fieldDef() {
        String fldname = field();
        return fieldType(fldname);
    }

    private Schema fieldType(String fldname) {
        Schema schema = new Schema();
        if (lex.matchKeyword("int")) {
            lex.eatKeyword("int");
            schema.addIntField(fldname);
        } else {
            lex.eatKeyword("varchar");
            lex.eatDelim('(');
            int strLen = lex.eatIntConstant();
            lex.eatDelim(')');
            schema.addStringField(fldname, strLen);
        }
        return schema;
    }

// Method for parsing create view commands

    public CreateViewData createView() {
        lex.eatKeyword("view");
        String viewname = lex.eatId();
        lex.eatKeyword("as");
        QueryData qd = query();
        return new CreateViewData(viewname, qd);
    }


//  Method for parsing create index commands

    public CreateIndexData createIndex() {
        lex.eatKeyword("index");
        String idxname = lex.eatId();
        lex.eatKeyword("on");
        String tblname = lex.eatId();
        lex.eatDelim('(');
        String fldname = field();
        lex.eatDelim(')');
        lex.eatKeyword("using");
        int idxtype = lex.eatIndexType();

        return new CreateIndexData(idxname, tblname, fldname, idxtype);
    }
}

