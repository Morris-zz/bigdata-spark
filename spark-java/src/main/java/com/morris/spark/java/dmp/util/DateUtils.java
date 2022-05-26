package com.morris.spark.java.dmp.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 日期时间工具类
 *
 * @author tony
 * @time 2017-3-10
 */
public class DateUtils {

	public static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
	public static final SimpleDateFormat DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd");
	public static final SimpleDateFormat DATEKEY_FORMAT2 = new SimpleDateFormat("yyyyMMddHHmmss");
	public static final SimpleDateFormat MINTIME = new SimpleDateFormat("yyyy-MM-dd HH:mm:00");

	/**
	 * 判断一天是否在另一天之前
	 *
	 * @param time1
	 *            第一个时间
	 * @param time2
	 *            第二个时间
	 * @return 判断结果
	 */
	public static boolean beforeDateFORMAT(String time1, String time2) {
		try {
			Date dateTime1 = DATEKEY_FORMAT.parse(time1);
			Date dateTime2 = DATEKEY_FORMAT.parse(time2);

			if (dateTime1.before(dateTime2)) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 判断一天是否在另一天之前
	 *
	 * @param time1
	 *            第一个时间
	 * @param time2
	 *            第二个时间
	 * @return 判断结果
	 */
	public static boolean beforeDate(String time1, String time2) {
		try {
			Date dateTime1 = DATE_FORMAT.parse(time1);
			Date dateTime2 = DATE_FORMAT.parse(time2);

			if (dateTime1.before(dateTime2)) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	
	/**
	 * 判断一个时间是否在另一个时间之前
	 *
	 * @param time1
	 *            第一个时间
	 * @param time2
	 *            第二个时间
	 * @return 判断结果
	 */
	public static boolean before(String time1, String time2) {
		try {
			Date dateTime1 = TIME_FORMAT.parse(time1);
			Date dateTime2 = TIME_FORMAT.parse(time2);

			if (dateTime1.before(dateTime2)) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 判断一个时间是否在另一个时间之后
	 *
	 * @param time1
	 *            第一个时间
	 * @param time2
	 *            第二个时间
	 * @return 判断结果
	 */
	public static boolean after(String time1, String time2) {
		try {
			Date dateTime1 = TIME_FORMAT.parse(time1);
			Date dateTime2 = TIME_FORMAT.parse(time2);

			if (dateTime1.after(dateTime2)) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 计算时间差值（单位为秒）
	 *
	 * @param time1
	 *            时间1
	 * @param time2
	 *            时间2
	 * @return 差值
	 */
	public static int minus(String time1, String time2) {
		try {
			Date datetime1 = TIME_FORMAT.parse(time1);
			Date datetime2 = TIME_FORMAT.parse(time2);

			long millisecond = datetime1.getTime() - datetime2.getTime();

			return Integer.valueOf(String.valueOf(millisecond / 1000));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * 计算时间差值（单位为秒）
	 *
	 * @param time1
	 *            时间1
	 * @param time2
	 *            时间2
	 * @return 差值DATEKEY_FORMAT
	 */
	public static int minusYYMMDD(String time1, String time2) {
		try {
			Date datetime1 = DATEKEY_FORMAT.parse(time1);
			Date datetime2 = DATEKEY_FORMAT.parse(time2);
			
			long millisecond = datetime1.getTime() - datetime2.getTime();
			
			return Integer.valueOf(String.valueOf(millisecond / 1000));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * 计算时间差值（单位为秒）
	 *
	 * @return 差值
	 */
	public static int minus(Date d1, Date d2) {
		try {
			long millisecond = d1.getTime() - d2.getTime();

			return Integer.valueOf(String.valueOf(millisecond / 1000));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * 计算时间差值（单位为毫秒）
	 *
	 * @return 差值
	 */
	public static int msec(Date d1, Date d2) {
		try {
			long millisecond = d1.getTime() - d2.getTime();

			return Integer.valueOf(String.valueOf(millisecond));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * yyyyMMdd 字符串的日期格式的计算
	 */
	public static int daysBetween(String smdate, String bdate) throws ParseException {
		if ("0".equals(smdate) || "0".equals(bdate)) {
			return 0;
		}

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Calendar cal = Calendar.getInstance();
		cal.setTime(sdf.parse(smdate));
		long time1 = cal.getTimeInMillis();
		cal.setTime(sdf.parse(bdate));
		long time2 = cal.getTimeInMillis();
		long between_days = (time2 - time1) / (1000 * 3600 * 24);

		return Integer.parseInt(String.valueOf(between_days));
	}

	/**
	 * yyyy-MM-dd 字符串的日期格式的计算
	 */
	public static int daysBetween2(String smdate, String bdate) throws ParseException {
		if ("0".equals(smdate) || "0".equals(bdate)) {
			return 0;
		}

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = Calendar.getInstance();
		cal.setTime(sdf.parse(smdate));
		long time1 = cal.getTimeInMillis();
		cal.setTime(sdf.parse(bdate));
		long time2 = cal.getTimeInMillis();
		long between_days = (time2 - time1) / (1000 * 3600 * 24);

		return Integer.parseInt(String.valueOf(between_days));
	}

	/**
	 * parm 1 yyyy-MM-dd parm 2 yyyyMM-d 字符串的日期格式的计算
	 */
	public static int daysBetween3(String smdate, String bdate) throws ParseException {
		if ("0".equals(smdate) || "0".equals(bdate)) {
			return 0;
		}

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
		Calendar cal = Calendar.getInstance();
		cal.setTime(sdf.parse(smdate));
		long time1 = cal.getTimeInMillis();
		cal.setTime(sdf2.parse(bdate));
		long time2 = cal.getTimeInMillis();
		long between_days = (time2 - time1) / (1000 * 3600 * 24);

		return Integer.parseInt(String.valueOf(between_days));
	}

	/**
	 * 获取年月日和小时
	 *
	 * @param datetime
	 *            时间（yyyy-MM-dd HH:mm:ss）
	 * @return 结果（yyyy-MM-dd_HH）
	 */
	public static String getDateHour(String datetime) {
		String date = datetime.split(" ")[0];
		String hourMinuteSecond = datetime.split(" ")[1];
		String hour = hourMinuteSecond.split(":")[0];
		return date + "_" + hour;
	}

	/**
	 * 获取当天日期（yyyy-MM-dd）
	 *
	 * @return 当天日期
	 */
	public static String getTodayDate() {
		return DATE_FORMAT.format(new Date());
	}
	
	/**
	 * 获取当天日期（yyyy-MM-dd HH:mm:ss）
	 *
	 * @return 当天日期
	 */
	public static String getTodayDateMMss() {
		return TIME_FORMAT.format(new Date());
	}

	/**
	 * 获取当天日期（yyyyMMdd）
	 *
	 * @return 当天日期
	 */
	public static String getTodayDate2() {
		return DATEKEY_FORMAT.format(new Date());
	}

	/**
	 * 获取当天日期（yyyy-MM-dd）
	 *
	 * @return 当天日期
	 */
	public static String getTodayDate(Date date) {
		return DATE_FORMAT.format(date);
	}

	/**
	 * 获取昨天的日期（yyyy-MM-dd）
	 *
	 * @return 昨天的日期
	 */
	public static String getYesterdayDate() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.DAY_OF_YEAR, -1);

		Date date = cal.getTime();

		return DATE_FORMAT.format(date);
	}

	/**
	 * yyyyMMdd 获取昨天的日期（yyyyMMdd）
	 *
	 * @return 昨天的日期
	 */
	public static String getYesterdayDate(String date) {
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(DATEKEY_FORMAT.parse(date));
		} catch (Exception e) {
			e.printStackTrace();
		}
		cal.add(Calendar.DAY_OF_YEAR, -1);

		Date yesterdayDate = cal.getTime();

		return DATEKEY_FORMAT.format(yesterdayDate);
	}

	/*
	 * yyyyMMdd 日期加几天或者减几天（yyyyMMdd）
	 *
	 */
	public static String getAddSubDate(String date, int number) {
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(DATEKEY_FORMAT.parse(date));
		} catch (Exception e) {
			e.printStackTrace();
		}
		cal.add(Calendar.DAY_OF_YEAR, number);

		Date yesterdayDate = cal.getTime();

		return DATEKEY_FORMAT.format(yesterdayDate);
	}

	/**
	 * yyyyMMdd 获取明天的日期（yyyyMMdd）
	 *
	 * @return 明天的日期
	 */
	public static String getTomorrowDate(String date) {
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(DATEKEY_FORMAT.parse(date));
		} catch (Exception e) {
			e.printStackTrace();
		}
		cal.add(Calendar.DAY_OF_YEAR, +1);

		Date yesterdayDate = cal.getTime();

		return DATEKEY_FORMAT.format(yesterdayDate);
	}

	/**
	 * yyyyMMdd 获取明天的日期（yyyy-MM-dd）
	 *
	 * @return 明天的日期
	 */
	public static String getTomorrow(String date) {
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(DATE_FORMAT.parse(date));
		} catch (Exception e) {
			e.printStackTrace();
		}
		cal.add(Calendar.DAY_OF_YEAR, +1);

		Date yesterdayDate = cal.getTime();

		return DATE_FORMAT.format(yesterdayDate);
	}

	/**
	 * 格式化日期（yyyy-MM-dd）
	 *
	 * @param date
	 *            Date对象
	 * @return 格式化后的日期
	 */
	public static String formatDate(Date date) {
		return DATE_FORMAT.format(date);
	}

	/**
	 * 格式化时间（yyyy-MM-dd HH:mm:ss）
	 *
	 * @param date
	 *            Date对象
	 * @return 格式化后的时间
	 */
	public static String formatTime(Date date) {
		return TIME_FORMAT.format(date);
	}

	/**
	 * 解析时间字符串
	 *
	 * @param time
	 *            时间字符串
	 * @return Date
	 */
	public static Date parseTime(String time) {
		try {
			return TIME_FORMAT.parse(time);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 格式化日期key(yyyyMMdd)
	 *
	 * @param date
	 * @return
	 */
	public static String formatDateKey(Date date) {
		return DATEKEY_FORMAT.format(date);
	}
	
	public static Date formatDateKey2(String date) throws ParseException {
		return DATEKEY_FORMAT2.parse(date);
	}

	/*
	 * 将时间戳(毫秒)转换为时间(yyyyMMdd)
	 */
	public static String stampToDateyyyyMMdd(String s) {
		String res;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
		long lt = NumberUtils.toLong(s);
		Date date = new Date(lt);
		res = simpleDateFormat.format(date);
		return res;
	}

	/*
	 * 将时间戳(秒)转换为时间(yyyyMMdd)
	 */
	public static String stampSecondToDate2yyyyMMdd(String s) {//
		if (!StringUtils.isNumeric(s) || "".equals(s)) {
			return getTodayDate2();
		}
		if ("0".equals(s)) {
			return "0";
		}
		String res;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
		long lt = NumberUtils.toLong(s);
		Date date = new Date(lt * 1000);
		res = simpleDateFormat.format(date);
		return res;
	}

	/*
	 * 将时间戳(秒)转换为时间(yyyy-MM-dd)
	 */
	public static String stampSecondToDate2yyyy_MM_dd(String s) {//
		if (!StringUtils.isNumeric(s) || "".equals(s)) {
			return getTodayDate2();
		}
		if ("0".equals(s)) {
			return "0";
		}
		String res;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		long lt = NumberUtils.toLong(s);
		Date date = new Date(lt * 1000);
		res = simpleDateFormat.format(date);
		return res;
	}

	/*
	 * 将时间戳(秒)转换为时间(yyyy-MM-dd)
	 */
	public static String stampSecondToDateyyyyMMdd(String s) {//
		String res;
		long lt = NumberUtils.toLong(s);
		Date date = new Date(lt * 1000);
		res = DATE_FORMAT.format(date);
		return res;
	}

	/*
	 * 将时间戳(毫秒)转换为时间(yyyy-MM-dd)
	 */
	public static String stampMillisecondToDateyyyyMMdd(String s) {//
		String res;
		long lt = NumberUtils.toLong(s);
		Date date = new Date(lt);
		res = DATE_FORMAT.format(date);
		return res;
	}

	/*
	 * 将时间戳(毫秒)转换为时间(yyyy-MM-dd HH:mm:ss)
	 */
	public static String stampToDate(String s) {
		String res;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long lt = new Long(s);
		Date date = new Date(lt);
		res = simpleDateFormat.format(date);
		return res;
	}

	/*
	 * 将时间戳(是秒)转换为小时(HH)
	 */
	public static String stampSecondToHour(String s) {
		String res;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH");
		long lt = new Long(String.valueOf(NumberUtils.toLong(s) * 1000));
		Date date = new Date(lt);
		res = simpleDateFormat.format(date);
		return res;
	}
	
	/*
	 * 将时间戳(是秒)转换为分钟(mm)
	 */
	public static String stampSecondToMm(String s) {
		String res;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("mm");
		long lt = new Long(String.valueOf(NumberUtils.toLong(s) * 1000));
		Date date = new Date(lt);
		res = simpleDateFormat.format(date);
		return res;
	}

	/*
	 * 将时间戳(是毫秒)转换为小时(HH)
	 */
	public static String stampMillisecondToHour(String s) {
		String res = "0";
		try {
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH");
			long lt = new Long(s);
			Date date = new Date(lt);
			res = simpleDateFormat.format(date);
		} catch (NumberFormatException e) {
			e.printStackTrace();
			res = "0";
		}
		return res;
	}
	
	/**
	 * 格式化日期key
	 *
	 * @param datekey
	 * @return
	 */
	public static Date parseDateKey(String datekey) {
		try {
			return DATEKEY_FORMAT.parse(datekey);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 格式化时间，保留到分钟级别 yyyyMMddHHmm
	 */
	public static String formatTimeMinute(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		return sdf.format(date);
	}

	/**
	 * 获取登录的时间
	 *
	 * @author tony
	 */
	public static String getLogTime(Long logTs) {
		String logTime = TIME_FORMAT.format(new Timestamp(logTs * 1000));
		return logTime;
	}

	/**
	 * 格式化时间 yyyy-MM-dd HH:mm:00
	 *
	 * @author tony
	 */
	public static String formatMintime(long ts) {
		return MINTIME.format(new Date(ts));
	}

	/**
	 * @Description: 把时间为20170702格式转成时间戳(秒)
	 * @author tony
	 */
	public static Long parseTime2ToStamp(String time) {
		try {
			return DATEKEY_FORMAT.parse(time).getTime() / 1000;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * @Description: 把时间为2017-07-2格式转成时间戳(秒)
	 * @author tony
	 */
	public static Long parseTimeToStamp(String time) {
		try {
			return DATE_FORMAT.parse(time).getTime() / 1000;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	
	/**
	 * @Description: 保留两位小数点
	 * @author tony
	 * @time 2017-8-11
	 */
	public static String formatDouble5(double d) {
		return String.format("%.2f", d);
	}

	/*
	 * 获取今天和第二天凌晨时间戳(毫秒) 例如:今天是2017-09-05,获取的是2017-09-06 00:00:00的时间戳
	 */
	public static int getNextdayMillisecond() {
		Calendar c = Calendar.getInstance();
		c.set(Calendar.HOUR_OF_DAY, 24);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);
		Long nextday = c.getTimeInMillis();

		long today = new Date().getTime();

		long time = nextday - today;

		int init = 86400;// 一天的时间戳(秒)

		if (time <= 0) {
			return init;
		} else {
			return (int) time / 1000;
		}

	}

	/**
	 * @throws Exception
	 * @Description: 获取之前的时间, 例如:参数是s:20180115,registDay:10 ,那么结果就返回20180105的日期的秒
	 * @author tony
	 * @time 2018-1-15
	 */
	public static long getRegistTime(String s, int registDay) throws Exception {
		Date endDate = null;
		try {
			SimpleDateFormat dft = new SimpleDateFormat("yyyyMMdd");

			Date parse = dft.parse(s);

			Calendar date = Calendar.getInstance();
			date.setTime(parse);
			date.set(Calendar.DATE, date.get(Calendar.DATE) - registDay);
			endDate = dft.parse(dft.format(date.getTime()));
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}

		return endDate.getTime() / 1000;

	}

	/**
	 * 获取昨天的日期（yyyyMMdd）
	 *
	 * @return 昨天的日期
	 */
	public static String getYesterday(String s) throws Exception {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat dft = new SimpleDateFormat("yyyyMMdd");

		Date parse = dft.parse(s);
		cal.setTime(parse);
		cal.add(Calendar.DAY_OF_YEAR, -1);

		Date date = cal.getTime();

		return DATEKEY_FORMAT.format(date);
	}

	/**
	 * 获取 距离今天多少天的date日期，返回yyyyMMdd
	 * 
	 * @param date
	 * @return
	 * @throws Exception
	 */
	public static String getBeforeDay(String s, int date) throws Exception {

		Calendar cal = Calendar.getInstance();
		SimpleDateFormat dft = new SimpleDateFormat("yyyyMMdd");
		Date parse = dft.parse(s);
		cal.setTime(parse);
		cal.add(Calendar.DAY_OF_YEAR, -date);

		Date dates = cal.getTime();

		return DATEKEY_FORMAT.format(dates);
	}
	
	/**
	 * 获取 距离今天多少天的date日期，返回yyyyMMdd
	 * 
	 * @param date
	 * @return
	 * @throws Exception
	 */
	public static long getBeforeMillisecond(String s, int date) throws Exception {

		Calendar cal = Calendar.getInstance();
		SimpleDateFormat dft = new SimpleDateFormat("yyyyMMdd");
		Date parse = dft.parse(s);
		cal.setTime(parse);
		cal.add(Calendar.DAY_OF_YEAR, -date);

		Date dates = cal.getTime();

		return dates.getTime();
	}

	/**
	 * 获取 距离今天多少天的date日期，返回yyyyMMdd
	 * 
	 * @param date
	 * @return
	 * @throws Exception
	 */
	public static String getBeforeDay(int date) throws Exception {
		Calendar cal = Calendar.getInstance();

		cal.add(Calendar.DAY_OF_YEAR, -date);

		Date dates = cal.getTime();

		return DATEKEY_FORMAT.format(dates);
	}

	
	public static boolean isInDate(String time ,String strDateBegin, String strDateEnd) throws Exception {
		//1543294799
		//1543298400
		long start = TIME_FORMAT.parse(strDateBegin).getTime() / 1000;
		long end = TIME_FORMAT.parse(strDateEnd).getTime() / 1000;
		
		
		if(NumberUtils.toLong(time)>=start && NumberUtils.toLong(time)<=end){
			return true;
		}
		
		return false;
	}
	
	public static String formatDate2(String date) throws Exception {
		Date datetime = DATEKEY_FORMAT.parse(date);
		return DATE_FORMAT.format(datetime);
	}
	
	public static void main(String[] args) {
		try {
			String s = "1592535246";
			String ts = DateUtils.stampToDateyyyyMMdd(s);
			System.out.println(ts);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
