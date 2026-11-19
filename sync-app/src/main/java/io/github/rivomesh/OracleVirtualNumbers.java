package io.github.rivomesh;

import io.github.rivomesh.cdc.Cell;
import io.github.rivomesh.cdc.TableSchema;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Exact integer arithmetic over physical Oracle NUMBER columns. */
final class OracleVirtualNumbers {
    private static final Pattern NAME=Pattern.compile("[A-Z][A-Z0-9_]{0,29}");
    private static final String OPERAND="\"([A-Z][A-Z0-9_]{0,29})\"";
    private static final Pattern ARITHMETIC=Pattern.compile(OPERAND
            +"\\h*(?:\\*\\h*([1-9][0-9]{0,2})|([+-])\\h*"+OPERAND+")");

    private OracleVirtualNumbers(){}

    static boolean isVirtual(TableSchema.Column column){
        return "VIRTUAL".equals(column.generationKind);
    }

    static Plan plan(TableSchema table){
        List<Formula> formulas=new ArrayList<>();
        for(int index=0;index<table.columns.size();index++){
            TableSchema.Column generated=table.columns.get(index);
            if(!isVirtual(generated))continue;
            if(!boundedNumber(generated)||!NAME.matcher(generated.name).matches())
                throw invalid("ORACLE_VIRTUAL_TYPE_UNSUPPORTED");
            if(table.primaryKey.contains(generated.name)||table.identityKey.contains(generated.name))
                throw invalid("ORACLE_VIRTUAL_PRIMARY_KEY_UNSUPPORTED");

            String expression=generated.generationExpression;
            if(expression==null||expression.length()>100)throw invalid("ORACLE_VIRTUAL_EXPRESSION_UNSUPPORTED");
            Matcher operands=ARITHMETIC.matcher(expression.trim());
            if(!operands.matches())throw invalid("ORACLE_VIRTUAL_EXPRESSION_UNSUPPORTED");
            Operand left=operand(table,operands.group(1));
            Operand right=null;
            int factor;
            BigInteger maximum;
            if(operands.group(2)!=null){
                factor=Integer.parseInt(operands.group(2));
                maximum=left.maximum().multiply(BigInteger.valueOf(factor));
            }else{
                right=operand(table,operands.group(4));
                factor="-".equals(operands.group(3))?-1:1;
                maximum=left.maximum().add(right.maximum());
            }
            if(maximum.compareTo(BigInteger.TEN.pow(generated.precision))>=0)
                throw invalid("ORACLE_VIRTUAL_PRECISION_UNSUPPORTED");
            formulas.add(new Formula(index,left,right,factor,generated.nullable));
        }
        return new Plan(table.columns.size()-formulas.size(),List.copyOf(formulas));
    }

    private static Operand operand(TableSchema table,String name){
        int index;
        try{index=table.column(name);}
        catch(IllegalArgumentException missing){throw invalid("ORACLE_VIRTUAL_EXPRESSION_UNSUPPORTED");}
        TableSchema.Column column=table.columns.get(index);
        if(column.generationKind!=null||!boundedNumber(column))
            throw invalid("ORACLE_VIRTUAL_OPERAND_UNSUPPORTED");
        return new Operand(index,BigInteger.TEN.pow(column.precision).longValueExact());
    }

    private static boolean boundedNumber(TableSchema.Column column){
        return "NUMBER".equals(column.sourceType)&&column.type==TableSchema.Type.INT64
                &&column.scale==0&&column.precision>=1&&column.precision<=18;
    }

    record Plan(int physicalColumns,List<Formula> formulas){
        void populate(List<Cell> row){
            for(Formula formula:formulas)row.set(formula.output,Cell.value(formula.value(row)));
        }

        void verifySnapshot(List<Cell> row){
            for(Formula formula:formulas){
                if(!java.util.Objects.equals(formula.value(row),row.get(formula.output).value))
                    throw invalid("ORACLE_VIRTUAL_SNAPSHOT_MISMATCH");
            }
        }
    }

    private record Operand(int index,long limit){
        BigInteger maximum(){return BigInteger.valueOf(limit-1);}

        Long value(List<Cell> row){
            Cell input=row.get(index);
            if(input==null||!input.present)throw invalid("ORACLE_FULL_ROW_IMAGE_REQUIRED");
            if(input.value==null)return null;
            long number;
            try{number=Long.parseLong(input.value);}
            catch(NumberFormatException unsupported){throw invalid("ORACLE_VIRTUAL_VALUE_OUT_OF_RANGE");}
            if(number<=-limit||number>=limit)throw invalid("ORACLE_VIRTUAL_VALUE_OUT_OF_RANGE");
            return number;
        }
    }

    private record Formula(int output,Operand left,Operand right,int factor,boolean nullable){
        String value(List<Cell> row){
            Long first=left.value(row);
            Long second=right==null?Long.valueOf(0):right.value(row);
            if(first==null||second==null){
                if(!nullable)throw invalid("ORACLE_REDO_UNEXPECTED_NULL");
                return null;
            }
            return Long.toString(right==null?first*factor:first+second*factor);
        }
    }

    private static IllegalStateException invalid(String code){
        return new IllegalStateException(code);
    }
}
