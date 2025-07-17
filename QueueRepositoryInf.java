package com.macauslot.repository;

import java.math.BigDecimal;
import java.util.List;

import com.macauslot.vo.NtwfundOutVo;
import com.macauslot.vo.QueueVo;

public interface QueueRepositoryInf {
	
	public List<QueueVo> getQueueRecordNew();
	public boolean getPromotionsCustomer(BigDecimal refId);
	public boolean getPromotionsFundInOutPromo(BigDecimal refId);
	public boolean getPromotionsICBCPromo(BigDecimal refId);
	
	public void addItem(BigDecimal refId,String refType, String queueType, String status, int attemptNumber, boolean isUnique);
	public void addLog(BigDecimal queueId,String status,String message);
	public void addStatusLog(BigDecimal queueId, String status);
	
	public boolean isRelationForQueueAndSMS(BigDecimal queueId);
	
	public List<QueueVo> getQueueSMS();
	public int updateQueue(BigDecimal queueId,String status);
	public int updateQueueAndIsSendedSMS(BigDecimal queueId,String status, String isSendedSMS);
	public int addError(BigDecimal queueId,String errorMsg);
	
	
	public List<QueueVo> getQueueEmail();
	
	public List<QueueVo> getQueueApiOrderDetailDC();
	public String getNextRefID();
	
	public void updateTxOBPaymentId(BigDecimal txId, String obPaymentId, String userId);
	public void updateTxStatus(BigDecimal txId, String status, String userId);
	public void updateQueueApi(BigDecimal queueID, String requestDetail, String responseDetail, String result);
	public void insertTransactionLog(BigDecimal txId, String log);
	public List<QueueVo> getQueueCashPayment();
	
	public List<QueueVo> getPromoBonusDC();
	
	public List<QueueVo> getPromoDCRebate();
	
	//fundoutmail
	public List<NtwfundOutVo> getNtwfundOutDetailOfXP(String express,String startTime,String endTime);
	public List<NtwfundOutVo> getNtwfundOutDetailOfCountry(String country,String startTime,String endTime);
	public List<NtwfundOutVo> getNtwfundOutDetailOfMoBank(String moBank,String startTime,String endTime);
	
	public List<NtwfundOutVo> getNtwfundOutDetailOfOT(String[] country,String startTime,String endTime);
	public List<NtwfundOutVo> getNtwfundOutDetailOfALL(String startTime,String endTime);
	public List<NtwfundOutVo> getNtwfundOutDetailOfMOOT(String[] mobank,String startTime,String endTime);
	
	
	public List<NtwfundOutVo> getNtwfundOutDetailOfEP_XLS(String express, String dateFrom,String dateTo);
	public List<NtwfundOutVo> getNtwfundOutDetailOfCountry_XLS(String country, String dateFrom,String dateTo);
	public List<NtwfundOutVo> getNtwfundOutDetailOfCountryBank_XLS(String mobank, String dateFrom,String dateTo);
	
	public List<NtwfundOutVo> getNtwfundOutDetailOfCountryOthers_XLS(String[] ct, String dateFrom,String dateTo);
	public List<NtwfundOutVo> getNtwfundOutDetailOfCountryBankOthers_XLS(String[] mb, String dateFrom,String dateTo);
	public List<NtwfundOutVo> getNtwfundOutDetailOfALL_XLS(String dateFrom,String dateTo);
	
	public int getBonuseQueueFlag();
}
