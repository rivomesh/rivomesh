package io.github.rivomesh;

import io.github.rivomesh.cdc.Cell;
import io.github.rivomesh.cdc.TableSchema;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Exact integer products over one physical Oracle NUMBER column. */
final class OracleVirtualNumbers {
    private static final Pattern NAME=Pattern.compile("[A-Z][A-Z0-9_]{0,29}");
    private static final Pattern PRODUCT=Pattern.compile("\"([A-Z][A-Z0-9_]{0,29})\"\\h*\\*\\h*([1-9][0-9]{0,2})");

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
            Matcher operands=PRODUCT.matcher(expression.trim());
            if(!operands.matches())throw invalid("ORACLE_VIRTUAL_EXPRESSION_UNSUPPORTED");
            String operandName=operands.group(1);
            int operandIndex;
            try{operandIndex=table.column(operandName);}
            catch(IllegalArgumentException missing){throw invalid("ORACLE_VIRTUAL_EXPRESSION_UNSUPPORTED");}
            TableSchema.Column operand=table.columns.get(operandIndex);
            if(operand.generationKind!=null||!boundedNumber(operand))
                throw invalid("ORACLE_VIRTUAL_OPERAND_UNSUPPORTED");

            int multiplier=Integer.parseInt(operands.group(2));
            BigInteger operandLimit=BigInteger.TEN.pow(operand.precision);
            BigInteger maximum=operandLimit.subtract(BigInteger.ONE).multiply(BigInteger.valueOf(multiplier));
            if(maximum.compareTo(BigInteger.TEN.pow(generated.precision))>=0)
                throw invalid("ORACLE_VIRTUAL_PRECISION_UNSUPPORTED");
            formulas.add(new Formula(index,operandIndex,multiplier,operandLimit.longValueExact(),generated.nullable));
        }
        return new Plan(table.columns.size()-formulas.size(),List.copyOf(formulas));
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

    private record Formula(int output,int operand,int multiplier,long operandLimit,boolean nullable){
        String value(List<Cell> row){
            Cell input=row.get(operand);
            if(input==null||!input.present)throw invalid("ORACLE_FULL_ROW_IMAGE_REQUIRED");
            if(input.value==null){
                if(!nullable)throw invalid("ORACLE_REDO_UNEXPECTED_NULL");
                return null;
            }
            long number;
            try{number=Long.parseLong(input.value);}
            catch(NumberFormatException unsupported){throw invalid("ORACLE_VIRTUAL_VALUE_OUT_OF_RANGE");}
            if(number<=-operandLimit||number>=operandLimit)throw invalid("ORACLE_VIRTUAL_VALUE_OUT_OF_RANGE");
            return Long.toString(number*multiplier);
        }
    }

    private static IllegalStateException invalid(String code){
        return new IllegalStateException(code);
    }
}
