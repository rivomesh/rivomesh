package io.github.rivomesh;

import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Locale;
import java.util.regex.*;

/** MySQL TIME is a signed duration, never a wall clock or epoch timestamp. */
public final class MysqlDuration {
    private static final long MAX=3020399L*1000000;
    private static final Pattern TEXT=Pattern.compile("([+-]?)([0-9]{1,3}):([0-5][0-9]):([0-5][0-9])(?:\\.([0-9]{1,6}))?");
    private MysqlDuration(){}
    public static String canonical(String value){
        if(value==null)return null;Matcher parts=TEXT.matcher(value);if(!parts.matches())throw new IllegalArgumentException("MYSQL_DURATION_INVALID");
        String fraction=parts.group(5);long micros=Long.parseLong(parts.group(2))*3600000000L+Long.parseLong(parts.group(3))*60000000L+Long.parseLong(parts.group(4))*1000000L;
        if(fraction!=null)micros+=Long.parseLong((fraction+"000000").substring(0,6));if(parts.group(1).equals("-"))micros=-micros;return format(micros);
    }
    public static String format(long micros){
        if(micros < -MAX||micros>MAX)throw new IllegalArgumentException("MYSQL_DURATION_RANGE_INVALID");
        long absolute=Math.abs(micros),seconds=absolute/1000000;
        return String.format(Locale.ROOT,"%s%02d:%02d:%02d.%06d",micros<0?"-":"",seconds/3600,seconds/60%60,seconds%60,absolute%1000000);
    }
    static long legacy(ByteArrayInputStream input)throws IOException{
        int number=input.readInteger(3);if((number&0x800000)!=0)number-=0x1000000;
        int absolute=Math.abs(number),hours=absolute/10000,minutes=absolute/100%100,seconds=absolute%100;
        return checked(number<0?-1:1,hours,minutes,seconds,0);
    }
    static long time2(int precision,ByteArrayInputStream input)throws IOException{
        if(precision<0||precision>6)throw new IOException("MYSQL_TIME_PRECISION_INVALID");
        int length=(precision+1)/2;long integer=bigEndian(input,3)-0x800000L,fraction=bigEndian(input,length),packed;
        if(length==3)packed=(integer<<24)+fraction;
        else{
            if(integer<0&&fraction!=0){integer++;fraction-=1L<<(8*length);}
            packed=(integer<<24)+fraction*(length==1?10000:length==2?100:0);
        }
        long absolute=Math.abs(packed),hms=absolute>>24,micros=absolute&0xffffffL;
        long result=checked(packed<0?-1:1,hms>>12,hms>>6&63,hms&63,micros);
        long quantum=1;for(int i=precision;i<6;i++)quantum*=10;
        if(result%quantum!=0)throw new IOException("MYSQL_TIME_FRACTION_INVALID");return result;
    }
    private static long bigEndian(ByteArrayInputStream input,int count)throws IOException{long result=0;for(int i=0;i<count;i++)result=result<<8|input.read();return result;}
    private static long checked(int sign,long hours,long minutes,long seconds,long fraction)throws IOException{
        long micros=(hours*3600+minutes*60+seconds)*1000000+fraction;
        if(hours>838||minutes>59||seconds>59||fraction>999999||micros>MAX)throw new IOException("MYSQL_TIME_BINARY_INVALID");return sign*micros;
    }
}
