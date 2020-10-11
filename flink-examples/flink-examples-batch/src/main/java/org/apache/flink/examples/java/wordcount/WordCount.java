/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.examples.java.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over text files.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link WordCountData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
public class WordCount {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataSet<String> text = null;
		if (params.has("input")) {
			// union all the inputs from text files
			for (String input : params.getMultiParameterRequired("input")) {
				if (text == null) {
					text = env.readTextFile(input);
				} else {
					text = text.union(env.readTextFile(input));
				}
			}
			Preconditions.checkNotNull(text, "Input DataSet should not be null.");
		} else {
			// get default test text data
			System.out.println("Executing WordCount example with default input data set.");
			System.out.println("Use --input to specify file input.");
			text = WordCountData.getDefaultTextLineDataSet(env);
		}

		DataSet<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new Tokenizer())
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0)
				.sum(1);

		// emit result
		if (params.has("output")) {
			counts.writeAsCsv(params.get("output"), "\n", " ");
			// execute program
			env.execute("WordCount Example");
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			counts.print();
		}

	}

//		public static void main(String[] args) throws Exception {
//		String str = "{\"activities\":[],\"balance\":0.00,\"cashierId\":\"10229\",\"cashierName\":\"3069_002\",\"changeAmount\":0,\"coupons\":[],\"createdBy\":\"10229\",\"createdTime\":\"2020-04-15 19:53:25\",\"deductQuota\":0.00,\"deleted\":false,\"giftCardPayments\":[],\"giftPoint\":0,\"id\":\"3069_XP200008666\",\"invoiceStatus\":\"NOT_INVOICE\",\"joinedActivities\":false,\"locked\":true,\"logisticsStatus\":\"RECEIVED\",\"mobilePayFlow\":[],\"onlineOrder\":false,\"orderSalesType\":\"NORMAL\",\"paidInAmount\":0,\"payId\":1250391774234505218,\"payIdPayments\":[],\"paymentStatus\":\"PAID\",\"paymentTime\":\"2020-04-15 23:50:04\",\"payments\":[],\"pointExchange\":0,\"posId\":\"2\",\"products\":[{\"availableInvoiceQuantity\":1,\"availablePromoList\":\"[]\",\"balance\":0.00,\"barcode\":\"303-007135\",\"brandId\":\"3001\",\"cancelledQuantity\":0,\"changedAmount\":false,\"checkPromoCodeList\":\"[]\",\"count\":1,\"createdBy\":\"10229\",\"createdTime\":\"2020-04-15 23:50:01\",\"deliveredQuantity\":1,\"discount\":0.00,\"exchange\":0,\"giftPoint\":0,\"goodsCode\":\"303-007135\",\"goodsNo\":\"141720444680001\",\"haitao\":0,\"id\":1250451315718451202,\"orderId\":\"3069_XP200008666\",\"pointAmount\":0,\"pointExchange\":0,\"productCode\":\"303-007135\",\"productName\":\"公主赠品帆布束口袋25X18CM开 25cm×18cm(开) 个\",\"productNo\":\"141720444680\",\"promoCodeList\":[],\"promoList\":[],\"refundedAmount\":0.00,\"returnedQuantity\":0,\"salePrice\":0.00,\"saleStoreId\":\"3069\",\"saleType\":\"NORMAL\",\"sellingPrice\":0.00,\"sequence\":1,\"shipperId\":\"3069\",\"shipperType\":\"STORE\",\"specification\":\"25cm×18cm(开)\",\"storeId\":\"3069\",\"tagPrice\":0.00,\"tax\":0.00,\"totalGiftPoint\":0,\"totalSellingPrice\":0.00,\"totalTagPrice\":0.00,\"unit\":\"PC\",\"updatedBy\":\"10229\",\"updatedTime\":\"2020-04-15 23:50:00\",\"vipDiscount\":0.00,\"zeroAmount\":0.00},{\"availableInvoiceQuantity\":30,\"availablePromoList\":\"[]\",\"balance\":0.00,\"barcode\":\"6907649768466\",\"brandId\":\"3001\",\"cancelledQuantity\":0,\"changedAmount\":false,\"checkPromoCodeList\":\"[]\",\"count\":30,\"createdBy\":\"10229\",\"createdTime\":\"2020-04-15 23:50:01\",\"deliveredQuantity\":30,\"discount\":0.00,\"exchange\":0,\"giftPoint\":0,\"goodsCode\":\"PX1010012\",\"goodsNo\":\"16925487748001\",\"haitao\":0,\"id\":1250451315726839809,\"orderId\":\"3069_XP200008666\",\"pointAmount\":0.00,\"pointExchange\":0,\"productCode\":\"PX1010012\",\"productName\":\"全棉红色通用礼盒(大) 435×335×82mm\",\"productNo\":\"16925487748\",\"promoCodeList\":[],\"promoList\":[],\"refundedAmount\":0.00,\"returnedQuantity\":0,\"salePrice\":0.00,\"saleStoreId\":\"3069\",\"saleType\":\"NORMAL\",\"sellingPrice\":0.00,\"sequence\":2,\"shipperId\":\"3069\",\"shipperType\":\"STORE\",\"specification\":\"435×335×82mm\",\"storeId\":\"3069\",\"tagPrice\":0.00,\"tax\":0.00,\"totalGiftPoint\":0,\"totalSellingPrice\":0.00,\"totalTagPrice\":0.00,\"unit\":\"PC\",\"updatedBy\":\"10229\",\"updatedTime\":\"2020-04-15 23:50:00\",\"vipDiscount\":0.00,\"zeroAmount\":0.00},{\"availableInvoiceQuantity\":100,\"availablePromoList\":\"[]\",\"balance\":0.00,\"barcode\":\"6907649768473\",\"brandId\":\"3001\",\"cancelledQuantity\":0,\"changedAmount\":false,\"checkPromoCodeList\":\"[]\",\"count\":100,\"createdBy\":\"10229\",\"createdTime\":\"2020-04-15 23:50:01\",\"deliveredQuantity\":100,\"discount\":0.00,\"exchange\":0,\"giftPoint\":0,\"goodsCode\":\"PX1010011\",\"goodsNo\":\"16919440587001\",\"haitao\":0,\"id\":1250451315731034114,\"orderId\":\"3069_XP200008666\",\"pointAmount\":0.00,\"pointExchange\":0,\"productCode\":\"PX1010011\",\"productName\":\"红色纸质购物手提袋 475×350×110mm\",\"productNo\":\"16919440587\",\"promoCodeList\":[],\"promoList\":[],\"refundedAmount\":0.00,\"returnedQuantity\":0,\"salePrice\":0.00,\"saleStoreId\":\"3069\",\"saleType\":\"NORMAL\",\"sellingPrice\":0.00,\"sequence\":3,\"shipperId\":\"3069\",\"shipperType\":\"STORE\",\"specification\":\"475×350×110mm\",\"storeId\":\"3069\",\"tagPrice\":0.00,\"tax\":0.00,\"totalGiftPoint\":0,\"totalSellingPrice\":0.00,\"totalTagPrice\":0.00,\"unit\":\"PC\",\"updatedBy\":\"10229\",\"updatedTime\":\"2020-04-15 23:50:00\",\"vipDiscount\":0.00,\"zeroAmount\":0.00},{\"availableInvoiceQuantity\":60,\"availablePromoList\":\"[]\",\"balance\":0.00,\"barcode\":\"6907649768497\",\"brandId\":\"3001\",\"cancelledQuantity\":0,\"changedAmount\":false,\"checkPromoCodeList\":\"[]\",\"count\":60,\"createdBy\":\"10229\",\"createdTime\":\"2020-04-15 23:50:01\",\"deliveredQuantity\":60,\"discount\":0.00,\"exchange\":0,\"giftPoint\":0,\"goodsCode\":\"PX1010008\",\"goodsNo\":\"16907853314001\",\"haitao\":0,\"id\":1250451315735228418,\"orderId\":\"3069_XP200008666\",\"pointAmount\":0.00,\"pointExchange\":0,\"productCode\":\"PX1010008\",\"productName\":\"全棉蓝色通用礼盒(大) 435×335×82mm\",\"productNo\":\"16907853314\",\"promoCodeList\":[],\"promoList\":[],\"refundedAmount\":0.00,\"returnedQuantity\":0,\"salePrice\":0.00,\"saleStoreId\":\"3069\",\"saleType\":\"NORMAL\",\"sellingPrice\":0.00,\"sequence\":4,\"shipperId\":\"3069\",\"shipperType\":\"STORE\",\"specification\":\"435×335×82mm\",\"storeId\":\"3069\",\"tagPrice\":0.00,\"tax\":0.00,\"totalGiftPoint\":0,\"totalSellingPrice\":0.00,\"totalTagPrice\":0.00,\"unit\":\"PC\",\"updatedBy\":\"10229\",\"updatedTime\":\"2020-04-15 23:50:00\",\"vipDiscount\":0.00,\"zeroAmount\":0.00},{\"availableInvoiceQuantity\":60,\"availablePromoList\":\"[]\",\"balance\":0.00,\"barcode\":\"6907649768510\",\"brandId\":\"3001\",\"cancelledQuantity\":0,\"changedAmount\":false,\"checkPromoCodeList\":\"[]\",\"count\":60,\"createdBy\":\"10229\",\"createdTime\":\"2020-04-15 23:50:01\",\"deliveredQuantity\":60,\"discount\":0.00,\"exchange\":0,\"giftPoint\":0,\"goodsCode\":\"PX1010006\",\"goodsNo\":\"16880819323001\",\"haitao\":0,\"id\":1250451315739422721,\"orderId\":\"3069_XP200008666\",\"pointAmount\":0.00,\"pointExchange\":0,\"productCode\":\"PX1010006\",\"productName\":\"蓝色纸质购物手提袋 475×350×110mm\",\"productNo\":\"16880819323\",\"promoCodeList\":[],\"promoList\":[],\"refundedAmount\":0.00,\"returnedQuantity\":0,\"salePrice\":0.00,\"saleStoreId\":\"3069\",\"saleType\":\"NORMAL\",\"sellingPrice\":0.00,\"sequence\":5,\"shipperId\":\"3069\",\"shipperType\":\"STORE\",\"specification\":\"475×350×110mm\",\"storeId\":\"3069\",\"tagPrice\":0.00,\"tax\":0.00,\"totalGiftPoint\":0,\"totalSellingPrice\":0.00,\"totalTagPrice\":0.00,\"unit\":\"PC\",\"updatedBy\":\"10229\",\"updatedTime\":\"2020-04-15 23:50:00\",\"vipDiscount\":0.00,\"zeroAmount\":0.00},{\"availableInvoiceQuantity\":500,\"availablePromoList\":\"[]\",\"balance\":0.00,\"barcode\":\"6952635001157\",\"brandId\":\"3001\",\"cancelledQuantity\":0,\"changedAmount\":false,\"checkPromoCodeList\":\"[]\",\"count\":500,\"createdBy\":\"10229\",\"createdTime\":\"2020-04-15 23:50:01\",\"deliveredQuantity\":500,\"discount\":0.00,\"exchange\":0,\"giftPoint\":0,\"goodsCode\":\"803-000135\",\"goodsNo\":\"12851459570001\",\"haitao\":0,\"id\":1250451315743617026,\"orderId\":\"3069_XP200008666\",\"pointAmount\":0.00,\"pointExchange\":0,\"productCode\":\"803-000135\",\"productName\":\"纯水湿巾 15cm×20cm 1片/袋\",\"productNo\":\"12851459570\",\"promoCodeList\":[],\"promoList\":[],\"refundedAmount\":0.00,\"returnedQuantity\":0,\"salePrice\":0.00,\"saleStoreId\":\"3069\",\"saleType\":\"NORMAL\",\"sellingPrice\":0.00,\"sequence\":6,\"shipperId\":\"3069\",\"shipperType\":\"STORE\",\"specification\":\"15cm×20cm\",\"storeId\":\"3069\",\"tagPrice\":0.00,\"tax\":0.00,\"totalGiftPoint\":0,\"totalSellingPrice\":0.00,\"totalTagPrice\":0.00,\"unit\":\"BAG\",\"updatedBy\":\"10229\",\"updatedTime\":\"2020-04-15 23:50:00\",\"vipDiscount\":0.00,\"zeroAmount\":0.00},{\"availableInvoiceQuantity\":400,\"availablePromoList\":\"[]\",\"balance\":0.00,\"barcode\":\"803-000105\",\"brandId\":\"3001\",\"cancelledQuantity\":0,\"changedAmount\":false,\"checkPromoCodeList\":\"[]\",\"count\":400,\"createdBy\":\"10229\",\"createdTime\":\"2020-04-15 23:50:01\",\"deliveredQuantity\":400,\"discount\":0.00,\"exchange\":0,\"giftPoint\":0,\"goodsCode\":\"803-000105\",\"goodsNo\":\"141753601703001\",\"haitao\":0,\"id\":1250451315747811329,\"orderId\":\"3069_XP200008666\",\"pointAmount\":0.00,\"pointExchange\":0,\"productCode\":\"803-000105\",\"productName\":\"新店开业小礼包 - 袋\",\"productNo\":\"141753601703\",\"promoCodeList\":[],\"promoList\":[],\"refundedAmount\":0.00,\"returnedQuantity\":0,\"salePrice\":0.00,\"saleStoreId\":\"3069\",\"saleType\":\"NORMAL\",\"sellingPrice\":0.00,\"sequence\":7,\"shipperId\":\"3069\",\"shipperType\":\"STORE\",\"specification\":\"-\",\"storeId\":\"3069\",\"tagPrice\":0.00,\"tax\":0.00,\"totalGiftPoint\":0,\"totalSellingPrice\":0.00,\"totalTagPrice\":0.00,\"unit\":\"PC\",\"updatedBy\":\"10229\",\"updatedTime\":\"2020-04-15 23:50:00\",\"vipDiscount\":0.00,\"zeroAmount\":0.00},{\"availableInvoiceQuantity\":50,\"availablePromoList\":\"[]\",\"balance\":0.00,\"barcode\":\"6952635075707\",\"brandId\":\"3001\",\"cancelledQuantity\":0,\"changedAmount\":false,\"checkPromoCodeList\":\"[]\",\"count\":50,\"createdBy\":\"10229\",\"createdTime\":\"2020-04-15 23:50:01\",\"deliveredQuantity\":50,\"discount\":0.00,\"exchange\":0,\"giftPoint\":0,\"goodsCode\":\"803-000093\",\"goodsNo\":\"12822082356001\",\"haitao\":0,\"id\":1250451315752005634,\"orderId\":\"3069_XP200008666\",\"pointAmount\":0.00,\"pointExchange\":0,\"productCode\":\"803-000093\",\"productName\":\"化妆棉四合一型 6cm×7cm-4P 6片/盒\",\"productNo\":\"12822082356\",\"promoCodeList\":[],\"promoList\":[],\"refundedAmount\":0.00,\"returnedQuantity\":0,\"salePrice\":0.00,\"saleStoreId\":\"3069\",\"saleType\":\"NORMAL\",\"sellingPrice\":0.00,\"sequence\":8,\"shipperId\":\"3069\",\"shipperType\":\"STORE\",\"specification\":\"6cm×7cm-4P\",\"storeId\":\"3069\",\"tagPrice\":0.00,\"tax\":0.00,\"totalGiftPoint\":0,\"totalSellingPrice\":0.00,\"totalTagPrice\":0.00,\"unit\":\"E10\",\"updatedBy\":\"10229\",\"updatedTime\":\"2020-04-15 23:50:00\",\"vipDiscount\":0.00,\"zeroAmount\":0.00},{\"availableInvoiceQuantity\":45,\"availablePromoList\":\"[]\",\"balance\":0.00,\"barcode\":\"6952635037057\",\"brandId\":\"3001\",\"cancelledQuantity\":0,\"changedAmount\":false,\"checkPromoCodeList\":\"[]\",\"count\":45,\"createdBy\":\"10229\",\"createdTime\":\"2020-04-15 23:50:01\",\"deliveredQuantity\":45,\"discount\":0.00,\"exchange\":0,\"giftPoint\":0,\"goodsCode\":\"800-002817\",\"goodsNo\":\"162090945192001\",\"haitao\":0,\"id\":1250451315756199938,\"orderId\":\"3069_XP200008666\",\"pointAmount\":0.00,\"pointExchange\":0,\"productCode\":\"800-002817\",\"productName\":\"全棉粉色通用印刷礼盒(大) 430x330x80mm\",\"productNo\":\"162090945192\",\"promoCodeList\":[],\"promoList\":[],\"refundedAmount\":0.00,\"returnedQuantity\":0,\"salePrice\":0.00,\"saleStoreId\":\"3069\",\"saleType\":\"NORMAL\",\"sellingPrice\":0.00,\"sequence\":9,\"shipperId\":\"3069\",\"shipperType\":\"STORE\",\"specification\":\"430x330x80mm\",\"storeId\":\"3069\",\"tagPrice\":0.00,\"tax\":0.00,\"totalGiftPoint\":0,\"totalSellingPrice\":0.00,\"totalTagPrice\":0.00,\"unit\":\"PC\",\"updatedBy\":\"10229\",\"updatedTime\":\"2020-04-15 23:50:00\",\"vipDiscount\":0.00,\"zeroAmount\":0.00},{\"availableInvoiceQuantity\":220,\"availablePromoList\":\"[]\",\"balance\":0.00,\"barcode\":\"6952635020592\",\"brandId\":\"3001\",\"cancelledQuantity\":0,\"changedAmount\":false,\"checkPromoCodeList\":\"[]\",\"count\":220,\"createdBy\":\"10229\",\"createdTime\":\"2020-04-15 23:50:01\",\"deliveredQuantity\":220,\"discount\":0.00,\"exchange\":0,\"giftPoint\":0,\"goodsCode\":\"800-002036\",\"goodsNo\":\"141389939238001\",\"haitao\":0,\"id\":1250451315760394242,\"imageUrl\":\"https://resource.pureh2b.com/omni/commdity/picture/commdity/800-002036/800-002036/6952635020592_z_01.jpg\",\"orderId\":\"3069_XP200008666\",\"pointAmount\":0.00,\"pointExchange\":0,\"productCode\":\"800-002036\",\"productName\":\"纸质购物手提袋 7.5cm 个\",\"productNo\":\"141389939238\",\"promoCodeList\":[],\"promoList\":[],\"refundedAmount\":0.00,\"returnedQuantity\":0,\"salePrice\":0.00,\"saleStoreId\":\"3069\",\"saleType\":\"NORMAL\",\"sellingPrice\":0.00,\"sequence\":10,\"shipperId\":\"3069\",\"shipperType\":\"STORE\",\"specification\":\"7.5cm\",\"storeId\":\"3069\",\"tagPrice\":0.00,\"tax\":0.00,\"totalGiftPoint\":0,\"totalSellingPrice\":0.00,\"totalTagPrice\":0.00,\"unit\":\"PC\",\"updatedBy\":\"10229\",\"updatedTime\":\"2020-04-15 23:50:00\",\"vipDiscount\":0.00,\"zeroAmount\":0.00},{\"availableInvoiceQuantity\":400,\"availablePromoList\":\"[]\",\"balance\":0.00,\"barcode\":\"6941318981770\",\"brandId\":\"3002\",\"cancelledQuantity\":0,\"changedAmount\":false,\"checkPromoCodeList\":\"[]\",\"count\":400,\"createdBy\":\"10229\",\"createdTime\":\"2020-04-15 23:50:01\",\"deliveredQuantity\":400,\"discount\":0.00,\"exchange\":0,\"giftPoint\":0,\"goodsCode\":\"4200017396-000\",\"goodsNo\":\"14936038185001\",\"haitao\":0,\"id\":1250451315764588546,\"orderId\":\"3069_XP200008666\",\"pointAmount\":0.00,\"pointExchange\":0,\"productCode\":\"4200017396-000\",\"productName\":\"全棉芯日用超薄体验装 245MM 1片/袋\",\"productNo\":\"14936038185\",\"promoCodeList\":[],\"promoList\":[],\"refundedAmount\":0.00,\"returnedQuantity\":0,\"salePrice\":0.00,\"saleStoreId\":\"3069\",\"saleType\":\"NORMAL\",\"sellingPrice\":0.00,\"sequence\":11,\"shipperId\":\"3069\",\"shipperType\":\"STORE\",\"specification\":\"245MM\",\"storeId\":\"3069\",\"tagPrice\":0.00,\"tax\":0.00,\"totalGiftPoint\":0,\"totalSellingPrice\":0.00,\"totalTagPrice\":0.00,\"unit\":\"PAC\",\"updatedBy\":\"10229\",\"updatedTime\":\"2020-04-15 23:50:00\",\"vipDiscount\":0.00,\"zeroAmount\":0.00}],\"purchaseOrderForm\":{\"memberBrand\":\"purcotton\",\"totalPrice\":0},\"returnPoint\":0,\"returnPointAmount\":0.00,\"returnType\":\"TO_STORE\",\"round\":false,\"sendCoupon\":false,\"source\":\"POS\",\"status\":\"COMPLETED\",\"stockLocked\":true,\"storeBrand\":\"purcotton\",\"storeId\":\"3069\",\"storeName\":\"P深圳海雅缤纷城店\",\"totalCount\":1866,\"totalDiscountPrice\":0.00,\"totalPrice\":0.00,\"totalTagPrice\":0.00,\"totalTax\":0.00,\"updatedBy\":\"10229\",\"updatedTime\":\"2020-04-15 23:50:04\",\"usedCoupons\":false,\"vipDiscount\":0.00,\"workday\":\"A\",\"zeroAmount\":0.00}";
//			long length = str.getBytes("UTF-8").length;
//			String printSize = getPrintSize(length);
//
//
//			String a = "3069_XP200008666";
//			long length1 = a.getBytes("UTF-8").length;
//			String printSize1 = getPrintSize(length1);
//			return;
//	}


	public static String getPrintSize(long size) {
		//如果字节数少于1024，则直接以B为单位，否则先除于1024，后3位因太少无意义
		if (size < 1024) {
			return String.valueOf(size) + "B";
		} else {
			size = size / 1024;
		}
		//如果原字节数除于1024之后，少于1024，则可以直接以KB作为单位
		//因为还没有到达要使用另一个单位的时候
		//接下去以此类推
		if (size < 1024) {
			return String.valueOf(size) + "KB";
		} else {
			size = size / 1024;
		}
		if (size < 1024) {
			//因为如果以MB为单位的话，要保留最后1位小数，
			//因此，把此数乘以100之后再取余
			size = size * 100;
			return String.valueOf((size / 100)) + "."
				+ String.valueOf((size % 100)) + "MB";
		} else {
			//否则如果要以GB为单位的，先除于1024再作同样的处理
			size = size * 100 / 1024;
			return String.valueOf((size / 100)) + "."
				+ String.valueOf((size % 100)) + "GB";
		}
	}


	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" ({@code Tuple2<String, Integer>}).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}

}
