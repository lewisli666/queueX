package com.macauslot.repository;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.stereotype.Repository;

import com.macauslot.util.EntityUtils;
import com.macauslot.vo.NtwfundOutVo;
import com.macauslot.vo.QueueVo;

import lombok.extern.slf4j.Slf4j;

@Repository
@Slf4j
public class QueueRepositoryImpl implements QueueRepositoryInf{

//	private static final Logger logger = LoggerFactory.getLogger(QueueRepositoryImpl.class);
	
    @PersistenceContext
    private EntityManager em;

	@Override
	public List<QueueVo> getQueueRecordNew() {
		String sql = " select cr_date,attempt_no,queue_id,ref_type, ref_id " 
					+ "   from queue " 
					+ "   where queue_type='NEWRECORD' " 
					+ "   and status not in ('S','P') and attempt_no <> 0 ";
		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql);
		List<QueueVo> list = EntityUtils.castEntity(query.getResultList(), QueueVo.class);
		return list;
	}
	

	@Override
	public boolean getPromotionsCustomer(BigDecimal refId) {
		String sql = "SELECT cust_id, cr_date, aff_id FROM dwh_custinfo where cust_id = :custId";
		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql);
		query.setParameter("custId",refId);
		List<Object[]> list= query.getResultList();
		if(list !=null && list.size() > 0) {
			if(list.get(0)[2] == null) {
				return false;
			}
			String affId = list.get(0)[2].toString();
			String crDate = list.get(0)[1].toString().substring(0, 19);
			
			StringBuffer sql2 = new StringBuffer();
			sql2.append(" select dwh_promotion.id as promotionID, bonus_amount, dwh_promotion.openbet_promo_id from dwh_promotion, ");
			sql2.append(" ( ");
			sql2.append(" SELECT dwh_promotion.ID, ");
			sql2.append(" (select count(*) from dwh_promotion_link where dwh_promotion_link.promotion_id = dwh_promotion.id) as Count ");
			sql2.append(" FROM dwh_promotion INNER JOIN  dwh_promotion_detail on dwh_promotion.id = dwh_promotion_detail.promotion_id and dwh_promotion_detail.name='AffiliateID' ");
			sql2.append(" where ");
			sql2.append(" value= :affId and  status='A' and type='AffiliateReg' ");
			sql2.append("  and start_date <= to_date(:crDate,'yyyy-mm-dd HH24:mi:ss') and end_date > to_date(:crDate,'yyyy-mm-dd HH24:mi:ss') ");
			sql2.append(" ) promotionitems ");
			sql2.append(" where dwh_promotion.ID = promotionitems.ID and promotionitems.Count < dwh_promotion.Quantity ");
			
			query = em.createNativeQuery(sql2.toString());
			query.setParameter("affId", affId);
			query.setParameter("crDate", crDate);
			log.info("param:[ affId:{},crDate:{} ]",affId,crDate);
			log.info("sql:"+sql2);
			List<Object[]> list2= query.getResultList();
			if(list2 != null && list2.size() > 0) {
				return true;
			}
			
		}
		
		return false;
	}


	@Override
	public boolean getPromotionsFundInOutPromo(BigDecimal refId) {
		String sql = "SELECT pmt_id, cr_date, amount, substr(unique_id,1,instr(unique_id,'_',1,1)-1) as type_name, payment_sort, acct_id FROM dwh_payinfo where pmt_id=:pmtId";
		log.info("sql:{}",sql);
		log.info("param:[refId:{}]",refId);
		Query query = em.createNativeQuery(sql);
		query.setParameter("pmtId",refId);
		List<Object[]> list= query.getResultList();
		if(list !=null && list.size() > 0) {
			if(list.get(0)[2] == null || list.get(0)[3] ==null ) {
				return false;
			}
			String affId = list.get(0)[2].toString();
			String crDate = list.get(0)[1].toString().substring(0, 19);
			
			StringBuffer sql2 = new StringBuffer();
			sql2.append(" select dwh_promotion.id as promotionID, bonus_amount, bonus_type, dwh_promotion.openbet_promo_id from dwh_promotion, ");
			sql2.append(" ( ");
			sql2.append(" SELECT dwh_promotion.ID, ");
			sql2.append(" (select count(*) from dwh_promotion_link where dwh_promotion_link.promotion_id = dwh_promotion.id) as Count ");
			sql2.append(" FROM dwh_promotion INNER JOIN  dwh_promotion_detail on dwh_promotion.id = dwh_promotion_detail.promotion_id and dwh_promotion_detail.name='EligibleAmount'  ");
			sql2.append(" where ");
			sql2.append(" value= :affId and  status='A' ");
			sql2.append(" type_name = :typeName  ");
			sql2.append("  and start_date <= to_date(:crDate,'yyyy-mm-dd hh24:mi:ss') and end_date > to_date(:crDate,'yyyy-mm-dd hh24:mi:ss')  ");
			sql2.append(" and type=:type) promotionitems ");
			sql2.append(" where dwh_promotion.ID = promotionitems.ID and promotionitems.Count < dwh_promotion.Quantity ");
			sql2.append(" AND ( ");
			sql2.append("      dwh_promotion.eligible_quantity=-1 OR ");
			sql2.append("      NOT EXISTS (");
			sql2.append("      select promotion_id, count(*) from dwh_promotion_link ");
			sql2.append("      INNER JOIN dwh_payinfo ON dwh_promotion_link.LINK_ID = dwh_payinfo.pmt_id and dwh_payinfo.ACCT_ID=:acctId ");
			sql2.append("      where promotion_id=dwh_promotion.id ");
			sql2.append("      group by promotion_id ");
			sql2.append("      having  count(*) >= dwh_promotion.eligible_quantity ");
			sql2.append("       )");
			sql2.append(" )");
			String payment_sort = list.get(0)[4].toString();
			log.info("type:payment_sort=D->FundIn payment_sort=W->FundOut");
			log.info("param:[ type:{},affId:{},typeName:{},crDate:{},acctId:{} ]",payment_sort,affId,list.get(0)[3],crDate,list.get(0)[5]);
			log.info("sql:"+sql2);
			query = em.createNativeQuery(sql2.toString());
			if("D".equals(payment_sort)) {
				query.setParameter("type", "FundIn");
			}else if("W".equals(payment_sort)){
				query.setParameter("type", "FundOut");
			}
			query.setParameter("affId", affId);
			query.setParameter("typeName", list.get(0)[3]);
			query.setParameter("crDate", crDate);
			query.setParameter("acctId", list.get(0)[5]);
			
			log.info(" Looking for promotion type of  " + list.get(0)[3] + " and " + ("D".equals(payment_sort)?"FundIn":"FundOut"));
			log.info(" Payment Amount:   "+list.get(0)[2]);//amount
			
			
			List<Object[]> list2= query.getResultList();
			
			if(list2 != null && list2.size() > 0) {
				return true;
			}
			
		}
		
		return false;
	}

	@Override
	public boolean getPromotionsICBCPromo(BigDecimal refId) {
		String sql = "SELECT p.pmt_id, p.cr_date, p.amount, substr(p.unique_id,1,instr(p.unique_id,'_',1,1)-1) as type_name, "
				+ " p.payment_sort, p.acct_id, nvl(m.display,'-') display, nvl(p.cnl_madj_id,-1) cnl_madj_id "
				+ " FROM dwh_payinfo p, dwh_manadj m "
				+ " where p.pmt_id=:pmtId "
				+ " and p.cnl_madj_id = m.madj_id(+) "
				+ " and substr(p.unique_id,1,instr(p.unique_id,'_',1,1)-1) in ('ICBC', 'BNU') ";
		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql);
		query.setParameter("pmtId",refId);
		List<Object[]> list= query.getResultList();
		
		if(list !=null && list.size() > 0) {
			if(Integer.valueOf(list.get(0)[7].toString()) > 0 && "N".equalsIgnoreCase(list.get(0)[6].toString())) {
				log.info("The payment "+ refId + "is cancelled with manadj id " + list.get(0)[7].toString());
				return false;
			}else {
				if(list.get(0)[2] == null || list.get(0)[3] == null) {
					return false;
				}
				String affId = list.get(0)[2].toString();
				String crDate = list.get(0)[1].toString().substring(0, 19);
				
				StringBuffer sql2 = new StringBuffer();
				sql2.append(" select dwh_promotion.id as promotionID, type_name, bonus_amount, bonus_type, eligible_amount from dwh_promotion, ");
				sql2.append(" ( ");
				sql2.append(" SELECT dwh_promotion.ID, value eligible_amount, ");
				sql2.append(" (select count(*) from dwh_promotion_link where dwh_promotion_link.promotion_id = dwh_promotion.id) as Count ");
				sql2.append(" FROM dwh_promotion INNER JOIN  dwh_promotion_detail on dwh_promotion.id = dwh_promotion_detail.promotion_id and dwh_promotion_detail.name='EligibleAmount' ");
				sql2.append(" where ");
				sql2.append(" value= :affId and  status='A' and type_name = :typeName ");
				sql2.append("  and start_date <= to_date(:crDate,'yyyy-mm-dd hh24:mi:ss') and end_date > to_date(:crDate,'yyyy-mm-dd hh24:mi:ss')  ");
				sql2.append(" and type=:type ) promotionitems ");
				sql2.append(" where dwh_promotion.ID = promotionitems.ID and promotionitems.Count < dwh_promotion.Quantity ");
				sql2.append(" AND ( ");
				sql2.append("   dwh_promotion.eligible_quantity=-1 OR  ");
				sql2.append("   NOT EXISTS ( ");
				sql2.append("   select promotion_id, count(*) from dwh_promotion_link ");
				sql2.append("   INNER JOIN dwh_payinfo ON dwh_promotion_link.LINK_ID = dwh_payinfo.pmt_id and dwh_payinfo.ACCT_ID=:acctId ");
				sql2.append("   where promotion_id=dwh_promotion.id ");
				sql2.append("   group by promotion_id  ");
				sql2.append("  having  count(*) >= dwh_promotion.eligible_quantity ");
				sql2.append("   ) ");
				sql2.append(" ) ");

				String payment_sort = list.get(0)[4].toString();
				log.info("type:payment_sort=D->FundIn payment_sort=W->FundOut");
				log.info("param:[ type:{},affId:{},typeName:{},crDate:{},acctId:{} ]",payment_sort,affId,list.get(0)[3],crDate,list.get(0)[5]);
				log.info("sql:"+sql2);
				query = em.createNativeQuery(sql2.toString());
				if("D".equals(payment_sort)) {
					query.setParameter("type", "FundIn");
				}else if("W".equals(payment_sort)){
					query.setParameter("type", "FundOut");
				}
				query.setParameter("affId", affId);
				query.setParameter("typeName", list.get(0)[3]);
				query.setParameter("crDate", crDate);
				query.setParameter("acctId", list.get(0)[5]);
				
				log.info(" Looking for promotion type of  " + list.get(0)[3] + " and " + ("D".equals(payment_sort)?"FundIn":"FundOut"));
				log.info(" Payment Amount:   "+list.get(0)[2]);//amount
				
				
				List<Object[]> list2= query.getResultList();
				
				if(list2 != null && list2.size() > 0) {
					return true;
				}
			}
			
		}
		
		return false;
	}

	@Modifying
	@Override
	public void addItem(BigDecimal refId, String refType, String queueType, String status, int attemptNumber,
			boolean isUnique) {
		String sql = " insert into queue (queue_type, status, ref_id, ref_type, attempt_no, uniqueness) values "
				+ " (:queue_type, :status, :ref_id, :ref_type, :attempt_no, :uniqueness) returning queue_id into :queue_id ";
		log.info("param:=[queue_type:{},status:{},ref_id:{},ref_type:{},attempt_no:{},uniqueness:{}]",refType,status,refId,refType,attemptNumber,isUnique);
		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql);
		
		query.setParameter("queue_type", queueType);
		query.setParameter("status", status);
		query.setParameter("ref_id", refId);
		query.setParameter("ref_type", refType);
		query.setParameter("attempt_no", attemptNumber);
		query.setParameter("uniqueness", isUnique?"Y":"N");
		
		query.executeUpdate();
		
	}


	@Modifying
	@Override
	public void addLog(BigDecimal queueId, String status, String message) {
		String sql = " insert into queue_log (queue_id, status, log_detail) values (:queue_id, :status, :log_detail) ";
		log.info("param:[queue_id:{},status:{},log_detail:{}]",queueId,status,message);
		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql);
		
		query.setParameter("queue_id", queueId);
		query.setParameter("status", status);
		query.setParameter("log_detail", message);
		
		query.executeUpdate();
	}


	@Modifying
	@Override
	public void addStatusLog(BigDecimal queueId, String status) {
		Date date = new Date();
		String sql = " insert into queue_log (queue_id, log_date, status) values (:queue_id, :log_date, :status) ";
		log.info("param:[queue_id:{},log_date:{},status:{}]",queueId,date,status);
		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql);
		query.setParameter("queue_id", queueId);
		query.setParameter("log_date", date);
		query.setParameter("status", status);
		
		query.executeUpdate();
	}

	@Override
	public boolean isRelationForQueueAndSMS(BigDecimal queueId) {
		String sql = " select * from queue q,queue_sms qs "
				+ " where q.queue_id = qs.queue_id and q.queue_type='SMS' "
				+ " and q.queue_id = :queueId ";

		log.info("param:[queue_id:{}]",queueId);
		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql);
		List<Object[]> list = query.getResultList();
		if( list!= null && list.size() > 0) {
			return true;
		}
		
		return false;
	}

	@Override
	public List<QueueVo> getQueueSMS() {
		String sql = " select q.cr_date as crDate,q.attempt_no as attemptNo,q.queue_id as queueId,qs.mobile,qs.sms_detail as smsDetail, q.status "
					+ " from queue q,queue_sms qs "
					+ " where q.queue_id = qs.queue_id and q.queue_type='SMS' "
//					+ " and q.status = 'S' and q.attempt_no <> 0 and is_sended_sms != 'Y' "; 當時忘記為什麼加字段
					+ " and q.status in ('F','A') and q.attempt_no <> 0 ";

		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql);
		List<QueueVo> list = EntityUtils.castEntity(query.getResultList(), QueueVo.class);
		return list;
	}
	@Modifying
	public int updateQueue(BigDecimal queueId,String status) {
		String sql = "update queue set status = :status";
		if(!"P".equalsIgnoreCase(status)) {
			sql += ", attempt_no = attempt_no - 1, instance_id = null ";
		}
		sql += ",last_attempt_date = sysdate " + " where queue_id = :queueId ";

		log.info("param:[status:{},queueId:{}]",status,queueId);
		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql);
		
		query.setParameter("status", status);
		query.setParameter("queueId", queueId);
		
		return query.executeUpdate();
	}
	@Modifying
	public int updateQueueAndIsSendedSMS(BigDecimal queueId,String status, String isSendedSMS) {
		String sql = "update queue set status = :status,is_sended_sms = :isSendedSMS ";
		sql += ", attempt_no = attempt_no - 1, instance_id = null ";
		sql += ",last_attempt_date = sysdate " + " where queue_id = :queueId ";

		log.info("param:[status:{},isSendedSMS:{},queueId:{}]",status,isSendedSMS,queueId);
		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql);
		
		query.setParameter("status", status);
		query.setParameter("isSendedSMS", isSendedSMS);
		query.setParameter("queueId", queueId);
		
		return query.executeUpdate();
	}
	@Modifying
	public int addError(BigDecimal queueId, String errorMsg) {
		String sql = " insert into queue_error (queue_id, error_msg) values (:queue_id, :error_msg) ";
		Query query = em.createNativeQuery(sql);

		log.info("param:[queue_id:{},error_msg:{}]",queueId,errorMsg);
		log.info("sql:{}",sql);
		query.setParameter("queue_id", queueId);
		query.setParameter("error_msg", errorMsg);

		return query.executeUpdate();
	}
    

	public List<QueueVo> getQueueEmail() {
		String sql =  " select q.cr_date as crDate,q.attempt_no as attemptNo,q.queue_id as queueId,qe.mail_to as mailTo,qe.mail_from as mailFrom, qe.mail_subject as mailSubject,qe.mail_body as mailBody, q.status " 
					+ "   from queue q,queue_email qe " 
					+ "   where q.queue_id = qe.queue_id and q.queue_type='EMAIL' " 
					+ "   and q.status in ('F','A') and q.attempt_no <> 0 ";

		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql);
		List<QueueVo> list = EntityUtils.castEntity(query.getResultList(), QueueVo.class);
		return list;
	}
	

	public List<QueueVo> getQueueApiOrderDetailDC() {
		StringBuffer sql = new StringBuffer("");
		sql.append(" select q.cr_date as crDate,q.attempt_no as attemptNo,q.queue_id as queueId, qa.acct_no as acctNo,e.exchange_desc as exchangeDesc, qd.value as dcAmount, qd2.value as openbetPromoId ");
		sql.append(" from queue q, queue_api qa,m_order_detail m,m_gift_list g,m_exchangetype e, queue_detail qd, queue_detail qd2 ");
		sql.append(" where q.queue_id = qa.queue_id  and q.queue_id = qd.queue_id and qd.name='Amount' and  m.order_detail_id = q.ref_id ");
		sql.append(" and m.gift_id = g.gift_id and m.exchange_id = e.exchange_id  and q.status not in ('S','P') ");
		sql.append(" and q.attempt_no <> 0  and q.queue_type = 'API' and ref_type = 'order_detail' ");
		sql.append(" and qd2.queue_id (+)= q.queue_id and qd2.name (+)= 'OpenbetPromoId' ");

		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql.toString());
		List<QueueVo> list = EntityUtils.castEntity(query.getResultList(), QueueVo.class);
		return list;
	}

	public String getNextRefID() {
		String sql = " select m_api_seq.nextval refid from dual ";

		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql);
		String refId= query.getSingleResult().toString();
		
		return refId;
	}

	public List<QueueVo> getQueueCashPayment() {
		StringBuffer sql = new StringBuffer("");
		sql.append(" select q.cr_date as crDate,q.attempt_no as attemptNo,q.queue_id as queueId, q.ref_id as refId,q.ref_type as refType ");
		sql.append(" ,t.tx_id as txId,t.acct_no as mTranAcctNo, t.channel, t.type, t.amount as mTranAmount, t.sort, t.currency, t.description, t.ref_code as refCode, t.ref_tx_id as refTxId ");
		sql.append(" from queue q,queue_api qa, m_transaction t  ");
		sql.append(" where q.queue_id = qa.queue_id ");
		sql.append(" and q.status not in ('S','P')  ");
		sql.append(" and q.queue_type = 'API' and q.ref_type = 'cash_payment' ");
		sql.append(" and t.tx_id = q.ref_id ");
		sql.append(" and t.status in ('Q', 'O') ");
		sql.append(" and q.attempt_no > 0 ");

		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql.toString());
		List<QueueVo> list = EntityUtils.castEntity(query.getResultList(), QueueVo.class);
		return list;
	}

	@Modifying
	public void updateTxOBPaymentId(BigDecimal txId, String obPaymentId, String userId) {
		String fieldName = "OB_PAYMENT_ID";
		String sql = " update m_transaction set " + fieldName + " = :fieldValue, last_update = sysdate, update_by = :userId where tx_id = :txId ";

		log.info("param:[userId:{},txId:{},fieldValue:{}]",userId,txId,obPaymentId);
		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql.toString());
		query.setParameter("userId", userId);
		query.setParameter("txId", txId);
		query.setParameter("fieldValue", obPaymentId);
		
		query.executeUpdate();
		
	}
	@Modifying
	public void updateTxStatus(BigDecimal txId, String status, String userId) {
		String fieldName = "STATUS";
		String sql =  " update m_transaction set " + fieldName + " = :fieldValue, last_update = sysdate, update_by = :userId where tx_id = :txId ";

		log.info("param:[userId:{},txId:{},fieldValue:{}]",userId,txId,status);
		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql.toString());
		query.setParameter("userId", userId);
		query.setParameter("txId", txId);
		query.setParameter("fieldValue", status);
		
		query.executeUpdate();
	}
	@Modifying
	public void updateQueueApi(BigDecimal queueID, String requestDetail, String responseDetail, String result) {
		
		String sql =  " update queue_api " + " set request_detail = :requestDetail, response_detail = :responseDetail,result = :result " + " where queue_id = :queueID ";

		log.info("param:[requestDetail:{},responseDetail:{},result:{},queueID:{}]",requestDetail,responseDetail,result,queueID);
		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql.toString());
		query.setParameter("requestDetail", requestDetail);
		query.setParameter("responseDetail", responseDetail);
		query.setParameter("result", result);
		query.setParameter("queueID", queueID);
		
		query.executeUpdate();
	}
	@Modifying
	public void insertTransactionLog(BigDecimal txId, String logdata) {
		String sql =   "insert into m_transaction_log " + "(tx_id,log,cr_date) " + "values " + "(:txId,:log,sysdate) ";

		log.info("param:[txId:{},log:{}]",txId,logdata);
		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql.toString());
		query.setParameter("txId", txId);
		query.setParameter("log", logdata);
		
		query.executeUpdate();
	}
	public List<QueueVo> getPromoBonusDC() {

		StringBuffer sql = new StringBuffer("");
		sql.append(" select q.cr_date as crDate,q.attempt_no as attemptNo,q.queue_id as queueId,qa.acct_no as acctNo, qa.type,q.ref_id as refId,q.ref_type as refType, ");
		sql.append(" prom.name, qd.value as bonusAmount, qd2.value as openbetPromoId");
		sql.append(" from queue q,queue_api qa, dwh_promotion_link pl, dwh_promotion prom, ");
		sql.append(" queue_detail qd, queue_detail qd2 ");
		sql.append(" where q.queue_id = qa.queue_id ");
		sql.append(" and q.queue_id = qd.queue_id and qd.name='Amount' ");
		sql.append(" and q.ref_id = pl.id ");
		sql.append(" and pl.promotion_id = prom.id ");
		sql.append(" and q.status not in ('S','P') and q.attempt_no <> 0  ");
		sql.append(" and q.queue_type = 'API' and ref_type = 'PROMODC' and qa.type = 'DC' and qd2.queue_id (+)= q.queue_id ");
		sql.append(" and qd2.name (+)= 'OpenbetPromoId' ");

		log.info("sql:{}",sql);
		Query query = em.createNativeQuery(sql.toString());
		List<QueueVo> list = EntityUtils.castEntity(query.getResultList(), QueueVo.class);
		return list;
	}

	public List<QueueVo> getPromoDCRebate() {

		StringBuffer sql = new StringBuffer("");
		sql.append(" select q.cr_date as crDate,q.attempt_no as attemptNo,q.queue_id as queueId,qa.acct_no as acctNo, ");
		sql.append(" qa.type,q.ref_id as refId,q.ref_type as refType, prom.name, qd.value as bonusAmount, pl.link_id as rebateId, qd2.value as openbetPromoId ");
		sql.append(" from queue q,queue_api qa, dwh_promotion_link pl, dwh_promotion prom, queue_detail qd, queue_detail qd2 ");
		sql.append(" where q.queue_id = qa.queue_id ");
		sql.append(" and q.queue_id = qd.queue_id ");
		sql.append(" and qd.name='Amount' and q.ref_id = pl.id and pl.promotion_id = prom.id ");
		sql.append(" and q.status not in ('S','P')  and q.attempt_no <> 0  and q.queue_type = 'API' ");
		sql.append(" and q.ref_type = 'PROMODCRebate' and qa.type = 'DC' and qd2.queue_id = q.queue_id ");
		sql.append(" and qd2.name = 'OpenbetPromoId' ");

		log.info("sql:"+sql.toString());
		Query query = em.createNativeQuery(sql.toString());
		List<QueueVo> list = EntityUtils.castEntity(query.getResultList(), QueueVo.class);
		return list;
	}
	
	//fundOutEmail
	public List<NtwfundOutVo> getNtwfundOutDetailOfXP(String express,String startTime,String endTime) {
		StringBuffer sql = new StringBuffer("");
		sql.append(" select distinct c.alt_name ,c.lname || ' '|| c.fname eng_name ");
		sql.append(" ,c.acct_no,p.amount tranc_amt ,e.INFO_VALUE gen_csv ,p.cr_date tranc_date ,c.fname ,c.lname ");
		sql.append(" ,c.addr_street_1 || ' '|| c.addr_street_2 || ' '|| c.addr_street_3 || ' '|| c.addr_street_4 || ' '|| c.addr_postcode || ' '|| c.country_code  as cust_address ");
		sql.append(" from dwh_payinfo p, dwh_custinfo c, m_payment_extra_info e ");
		sql.append(" where p.cr_date >= to_date(:dateFrom,'yyyy-mm-dd hh24:mi:ss') ");
		sql.append(" and p.cr_date <= to_date(:dateTo,'yyyy-mm-dd hh24:mi:ss') ");
		sql.append(" and p.acct_id = c.acct_id ");
		sql.append(" and p.payment_sort = 'W' ");
		sql.append(" and p.pmt_id = e.pmt_id(+) ");
		sql.append(" and e.info_name (+)= 'GEN_CSV' ");
		sql.append(" and not exists ( select j.madj_id from dwh_manadj j where j.madj_id = p.cnl_madj_id and j.display = 'N') ");
		sql.append(" and P.PAY_MTHD= :express  ");
		sql.append(" order by c.acct_no,tranc_amt ");

		log.info("param:[dateFrom:{},dateTo:{},express:{}]",startTime,endTime,express);
		log.info("sql:"+sql.toString());
		Query query = em.createNativeQuery(sql.toString());
		query.setParameter("dateFrom", startTime);
		query.setParameter("dateTo", endTime);
		query.setParameter("express", express);
		
		List<NtwfundOutVo> list = EntityUtils.mapping(query.getResultList(), NtwfundOutVo.class, "getNtwfundOutDetailOfXP");
		
		return list;
	}
	
	@Override
	public List<NtwfundOutVo> getNtwfundOutDetailOfCountry(String country,String startTime,String endTime) {
		StringBuffer sql = new StringBuffer("");
		sql.append(" select distinct c.alt_name,c.lname || ' '|| c.fname eng_name ,c.acct_no ");
		sql.append(" ,mb.bank_name ,p.amount tranc_amt,mb.bank_ccy_code currency  ");
		sql.append(" ,mb.bank_acct_no bank_acct_no,p.cr_date tranc_date,mb.country_code ,e.info_name ");
		sql.append(" ,c.lname,mb.bank_ccy_name  ,c.fname  ,P.pay_mthd ,e.INFO_VALUE gen_csv ,P.AUTH_CODE,e.pmt_id tranc_id ");
		sql.append(" from dwh_payinfo p, dwh_custinfo c, m_payment_extra_info e,dwh_tcpmbank mb ");
		sql.append(" where p.cr_date >= to_date(:dateFrom,'yyyy-mm-dd hh24:mi:ss') ");
		sql.append(" and p.cr_date <= to_date(:dateTo,'yyyy-mm-dd hh24:mi:ss') ");
		sql.append(" and p.acct_id = c.acct_id ");
		sql.append(" and p.payment_sort = 'W' ");
		sql.append("  and p.pmt_id = e.pmt_id(+)  ");
		sql.append(" and e.info_name (+)= 'GEN_CSV' ");
		sql.append(" and p.cpm_id=mb.CPM_ID ");
		sql.append(" and not exists ( select j.madj_id from dwh_manadj j where j.madj_id = p.cnl_madj_id and j.display = 'N') ");
		sql.append(" and mb.COUNTRY_CODE= :country ");
		sql.append(" order by mb.COUNTRY_CODE,p.cr_date,mb.BANK_NAME,c.acct_no,tranc_amt ");

		log.info("param:[dateFrom:{},dateTo:{},country:{}]",startTime,endTime,country);
		log.info("sql:"+sql.toString());
		Query query = em.createNativeQuery(sql.toString());
		query.setParameter("dateFrom", startTime);
		query.setParameter("dateTo", endTime);
		query.setParameter("country", country);
		
		List<NtwfundOutVo> list = EntityUtils.mapping(query.getResultList(), NtwfundOutVo.class, "getNtwfundOutDetailOfCountry");
		
		return list;
	}
	@Override
	public List<NtwfundOutVo> getNtwfundOutDetailOfMoBank(String moBank, String startTime, String endTime) {
		StringBuffer sql = new StringBuffer("");
		sql.append(" select distinct c.alt_name,c.lname || ' '|| c.fname eng_name ");
		sql.append(" ,c.acct_no,mb.BANK_NAME,mb.bank_ccy_code currency ,p.amount tranc_amt ");
		sql.append(" ,mb.bank_acct_no bank_acct_no ,p.cr_date tranc_date,mb.COUNTRY_CODE ");
		sql.append(" ,e.info_name,c.lname,c.fname ,P.PAY_MTHD ,e.INFO_VALUE GEN_CSV ");
		sql.append(" ,P.AUTH_CODE,e.pmt_id  as TRANC_ID ");
		sql.append(" from dwh_payinfo p, dwh_custinfo c, m_payment_extra_info e,dwh_tcpmbank mb ");
		sql.append(" where p.cr_date >= to_date(:dateFrom,'yyyy-mm-dd hh24:mi:ss') ");
		sql.append(" and p.cr_date <= to_date(:dateTo,'yyyy-mm-dd hh24:mi:ss') ");
		sql.append(" and p.acct_id = c.acct_id ");
		sql.append(" and p.payment_sort = 'W' ");
		sql.append(" and p.pmt_id = e.pmt_id(+) ");
		sql.append(" and e.info_name (+)= 'GEN_CSV' ");
		sql.append(" and p.cpm_id=mb.CPM_ID ");
		sql.append(" and not exists ( select j.madj_id from dwh_manadj j where j.madj_id = p.cnl_madj_id and j.display = 'N') ");
		sql.append(" and mb.COUNTRY_CODE || '-' || mb.BANK_PREFIX  = :moBank  ");
		sql.append(" order by c.acct_no,tranc_amt,mb.BANK_NAME ");

		log.info("param:[dateFrom:{},dateTo:{},moBank:{}]",startTime,endTime,moBank);
		log.info("sql:"+sql.toString());
		Query query = em.createNativeQuery(sql.toString());
		query.setParameter("dateFrom", startTime);
		query.setParameter("dateTo", endTime);
		query.setParameter("moBank", moBank);
		
		List<NtwfundOutVo> list = EntityUtils.mapping(query.getResultList(), NtwfundOutVo.class, "getNtwfundOutDetailOfMoBank");
		
		return list;
	}
	@Override
	public List<NtwfundOutVo> getNtwfundOutDetailOfOT(String[] country, String startTime, String endTime) {
		String strSql = "";
		for(String str : country) {
			strSql+=",'"+str+"'";
		}
		StringBuffer sql = new StringBuffer("");
		sql.append(" select distinct c.alt_name,c.lname || ' '|| c.fname eng_name ");
		sql.append(" ,c.acct_no,mb.BANK_NAME ,mb.bank_ccy_code currency ");
		sql.append(" ,p.amount tranc_amt,mb.bank_acct_no bank_acct_no ");
		sql.append(" ,p.cr_date tranc_date,mb.COUNTRY_CODE ,e.info_name ");
		sql.append(" ,c.lname,c.fname ,P.PAY_MTHD ,e.INFO_VALUE GEN_CSV ");
		sql.append(" ,P.AUTH_CODE,e.pmt_id  as TRANC_ID ");
		sql.append(" from dwh_payinfo p, dwh_custinfo c, m_payment_extra_info e,dwh_tcpmbank mb ");
		sql.append(" where p.cr_date >= to_date(:dateFrom,'yyyy-mm-dd hh24:mi:ss') ");
		sql.append(" and p.cr_date <= to_date(:dateTo,'yyyy-mm-dd hh24:mi:ss')");
		sql.append(" and p.acct_id = c.acct_id ");
		sql.append(" and p.payment_sort = 'W' ");
		sql.append(" and p.pmt_id = e.pmt_id(+) ");
		sql.append(" and e.info_name (+)= 'GEN_CSV' ");
		sql.append(" and p.cpm_id=mb.CPM_ID ");
		sql.append(" and not exists (select j.madj_id from dwh_manadj j where j.madj_id = p.cnl_madj_id and j.display = 'N') ");
		sql.append(" and mb.COUNTRY_CODE not in ( " + strSql.substring(1) + ")");
		sql.append(" order by mb.COUNTRY_CODE,p.cr_date,mb.BANK_NAME,c.acct_no,tranc_amt ");

		log.info("param:[dateFrom:{},dateTo:{}]",startTime,endTime);
		log.info("sql:"+sql.toString());
		Query query = em.createNativeQuery(sql.toString());
		query.setParameter("dateFrom", startTime);
		query.setParameter("dateTo", endTime);
		
		List<NtwfundOutVo> list = EntityUtils.mapping(query.getResultList(), NtwfundOutVo.class, "getNtwfundOutDetailOfMoBank");
		
		return list;
	}
	
	@Override
	public List<NtwfundOutVo> getNtwfundOutDetailOfALL(String startTime, String endTime) {
		StringBuffer sql = new StringBuffer("");
		sql.append(" select distinct c.alt_name,c.lname || ' '|| c.fname eng_name ");
		sql.append(" ,c.acct_no,mb.BANK_NAME ,mb.bank_ccy_code currency ");
		sql.append(" ,p.amount tranc_amt,mb.bank_acct_no bank_acct_no ");
		sql.append(" ,p.cr_date tranc_date,mb.COUNTRY_CODE ,e.info_name ");
		sql.append(" ,c.lname,c.fname ,P.PAY_MTHD ,e.INFO_VALUE GEN_CSV ");
		sql.append(" ,P.AUTH_CODE,e.pmt_id  as TRANC_ID ");
		sql.append(" from dwh_payinfo p, dwh_custinfo c, m_payment_extra_info e,dwh_tcpmbank mb ");
		sql.append(" where p.cr_date >= to_date(:dateFrom,'yyyy-mm-dd hh24:mi:ss') ");
		sql.append(" and p.cr_date <= to_date(:dateTo,'yyyy-mm-dd hh24:mi:ss')");
		sql.append(" and p.acct_id = c.acct_id ");
		sql.append(" and p.payment_sort = 'W' ");
		sql.append(" and p.pmt_id = e.pmt_id(+) ");
		sql.append(" and e.info_name (+)= 'GEN_CSV' ");
		sql.append(" and p.cpm_id=mb.CPM_ID ");
		sql.append(" and not exists (select j.madj_id from dwh_manadj j where j.madj_id = p.cnl_madj_id and j.display = 'N') ");
		sql.append(" order by mb.COUNTRY_CODE,p.cr_date,mb.BANK_NAME,c.acct_no,tranc_amt ");

		log.info("param:[dateFrom:{},dateTo:{}]",startTime,endTime);
		log.info("sql:"+sql.toString());
		Query query = em.createNativeQuery(sql.toString());
		query.setParameter("dateFrom", startTime);
		query.setParameter("dateTo", endTime);
		
		List<NtwfundOutVo> list = EntityUtils.mapping(query.getResultList(), NtwfundOutVo.class, "getNtwfundOutDetailOfMoBank");
		
		return list;
	}
	@Override
	public List<NtwfundOutVo> getNtwfundOutDetailOfMOOT(String[] mobank, String startTime, String endTime) {
		String strSql = "";
		for(String str : mobank) {
			strSql+=",'"+str+"'";
		}
		StringBuffer sql = new StringBuffer("");
		sql.append(" select distinct c.alt_name,c.lname || ' '|| c.fname eng_name ");
		sql.append(" ,c.acct_no,mb.BANK_NAME ,mb.bank_ccy_code currency ");
		sql.append(" ,p.amount tranc_amt,mb.bank_acct_no bank_acct_no ");
		sql.append(" ,p.cr_date tranc_date,mb.COUNTRY_CODE ,e.info_name ");
		sql.append(" ,c.lname,c.fname ,P.PAY_MTHD ,e.INFO_VALUE GEN_CSV ");
		sql.append(" ,P.AUTH_CODE,e.pmt_id  as TRANC_ID ");
		sql.append(" from dwh_payinfo p, dwh_custinfo c, m_payment_extra_info e,dwh_tcpmbank mb ");
		sql.append(" where p.cr_date >= to_date(:dateFrom,'yyyy-mm-dd hh24:mi:ss') ");
		sql.append(" and p.cr_date <= to_date(:dateTo,'yyyy-mm-dd hh24:mi:ss')");
		sql.append(" and p.acct_id = c.acct_id ");
		sql.append(" and p.payment_sort = 'W' ");
		sql.append(" and p.pmt_id = e.pmt_id(+) ");
		sql.append(" and e.info_name (+)= 'GEN_CSV' ");
		sql.append(" and p.cpm_id=mb.CPM_ID ");
		sql.append(" and not exists (select j.madj_id from dwh_manadj j where j.madj_id = p.cnl_madj_id and j.display = 'N') ");
		sql.append(" and mb.COUNTRY_CODE='MO' ");
		sql.append(" and mb.COUNTRY_CODE || '-' || mb.BANK_PREFIX  not in ( " + strSql.substring(1) + ")");
		sql.append(" order by c.acct_no,tranc_amt,mb.BANK_NAME ");

		log.info("param:[dateFrom:{},dateTo:{}]",startTime,endTime);
		log.info("sql:"+sql.toString());
		Query query = em.createNativeQuery(sql.toString());
		query.setParameter("dateFrom", startTime);
		query.setParameter("dateTo", endTime);
		
		List<NtwfundOutVo> list = EntityUtils.mapping(query.getResultList(), NtwfundOutVo.class, "getNtwfundOutDetailOfMoBank");
		
		return list;
	}
	@Override
	public List<NtwfundOutVo> getNtwfundOutDetailOfEP_XLS(String express, String dateFrom, String dateTo) {
		StringBuffer sql = new StringBuffer("");
		sql.append(" select distinct c.alt_name ,c.lname || ' '|| c.fname eng_name ");
		sql.append(" ,c.acct_no,p.amount tranc_amt ,e.INFO_VALUE gen_csv ,p.cr_date tranc_date ,c.fname ,c.lname ");
		sql.append(" ,c.addr_street_1 || ' '|| c.addr_street_2 || ' '|| c.addr_street_3 || ' '|| c.addr_street_4 || ' '|| c.addr_postcode || ' '|| c.country_code  as cust_address ");
		sql.append(" from dwh_payinfo p, dwh_custinfo c, m_payment_extra_info e ");
		sql.append(" where p.cr_date >= to_date(:dateFrom,'yyyy-mm-dd hh24:mi:ss') ");
		sql.append(" and p.cr_date <= to_date(:dateTo,'yyyy-mm-dd hh24:mi:ss') ");
		sql.append(" and p.acct_id = c.acct_id ");
		sql.append(" and p.payment_sort = 'W' ");
		sql.append(" and p.pmt_id = e.pmt_id(+) ");
		sql.append(" and e.info_name (+)= 'GEN_CSV' ");
		sql.append(" and not exists ( select j.madj_id from dwh_manadj j where j.madj_id = p.cnl_madj_id and j.display = 'N') ");
		sql.append(" and P.PAY_MTHD= :express  ");
		sql.append(" order by c.acct_no,tranc_amt ");

		log.info("param:[dateFrom:{},dateTo:{},express:{}]",dateFrom,dateTo,express);
		log.info("sql:"+sql.toString());
		Query query = em.createNativeQuery(sql.toString());
		query.setParameter("dateFrom", dateFrom);
		query.setParameter("dateTo", dateTo);
		query.setParameter("express", express);
		
		List<NtwfundOutVo> list = EntityUtils.mapping(query.getResultList(), NtwfundOutVo.class, "getNtwfundOutDetailOfXP");
		
		return list;
	}
	@Override
	public List<NtwfundOutVo> getNtwfundOutDetailOfCountry_XLS(String country, String dateFrom, String dateTo) {
		StringBuffer sql = new StringBuffer("");
		sql.append(" select distinct c.alt_name,c.lname || ' '|| c.fname eng_name ,c.acct_no ");
		sql.append(" ,mb.bank_name ,p.amount tranc_amt,mb.bank_ccy_code currency  ");
		sql.append(" ,mb.bank_acct_no bank_acct_no,p.cr_date tranc_date,mb.country_code ,e.info_name ");
		sql.append(" ,c.lname,mb.bank_ccy_name  ,c.fname  ,P.pay_mthd ,e.INFO_VALUE gen_csv ,P.AUTH_CODE,e.pmt_id tranc_id ");
		sql.append(" from dwh_payinfo p, dwh_custinfo c, m_payment_extra_info e,dwh_tcpmbank mb ");
		sql.append(" where p.cr_date >= to_date(:dateFrom,'yyyy-mm-dd hh24:mi:ss') ");
		sql.append(" and p.cr_date <= to_date(:dateTo,'yyyy-mm-dd hh24:mi:ss') ");
		sql.append(" and p.acct_id = c.acct_id ");
		sql.append(" and p.payment_sort = 'W' ");
		sql.append("  and p.pmt_id = e.pmt_id(+)  ");
		sql.append(" and e.info_name (+)= 'GEN_CSV' ");
		sql.append(" and p.cpm_id=mb.CPM_ID ");
		sql.append(" and not exists ( select j.madj_id from dwh_manadj j where j.madj_id = p.cnl_madj_id and j.display = 'N') ");
		sql.append(" and mb.COUNTRY_CODE= :country ");
		sql.append(" order by mb.COUNTRY_CODE,p.cr_date,mb.BANK_NAME,c.acct_no,tranc_amt ");

		log.info("param:[dateFrom:{},dateTo:{},country:{}]",dateFrom,dateTo,country);
		log.info("sql:"+sql.toString());
		Query query = em.createNativeQuery(sql.toString());
		query.setParameter("dateFrom", dateFrom);
		query.setParameter("dateTo", dateTo);
		query.setParameter("country", country);
		
		List<NtwfundOutVo> list = EntityUtils.mapping(query.getResultList(), NtwfundOutVo.class, "getNtwfundOutDetailOfCountry");
		
		return list;
	}
	@Override
	public List<NtwfundOutVo> getNtwfundOutDetailOfCountryBank_XLS(String mobank, String dateFrom, String dateTo) {
		StringBuffer sql = new StringBuffer("");
		sql.append(" select distinct c.alt_name,c.lname || ' '|| c.fname eng_name ");
		sql.append(" ,c.acct_no,mb.BANK_NAME ,mb.bank_ccy_code currency ");
		sql.append(" ,p.amount tranc_amt,mb.bank_acct_no bank_acct_no ");
		sql.append(" ,p.cr_date tranc_date,mb.COUNTRY_CODE ,e.info_name ");
		sql.append(" ,c.lname,c.fname ,P.PAY_MTHD ,e.INFO_VALUE GEN_CSV ");
		sql.append(" ,P.AUTH_CODE,e.pmt_id  as TRANC_ID ");
		sql.append(" from dwh_payinfo p, dwh_custinfo c, m_payment_extra_info e,dwh_tcpmbank mb ");
		sql.append(" where p.cr_date >= to_date(:dateFrom,'yyyy-mm-dd hh24:mi:ss') ");
		sql.append(" and p.cr_date <= to_date(:dateTo,'yyyy-mm-dd hh24:mi:ss')");
		sql.append(" and p.acct_id = c.acct_id ");
		sql.append(" and p.payment_sort = 'W' ");
		sql.append(" and p.pmt_id = e.pmt_id(+) ");
		sql.append(" and e.info_name (+)= 'GEN_CSV' ");
		sql.append(" and p.cpm_id=mb.CPM_ID ");
		sql.append(" and not exists (select j.madj_id from dwh_manadj j where j.madj_id = p.cnl_madj_id and j.display = 'N') ");
		sql.append(" and mb.COUNTRY_CODE='MO' ");
		sql.append(" and mb.COUNTRY_CODE || '-' || mb.BANK_PREFIX =:mobank ");
		sql.append(" order by c.acct_no,tranc_amt,mb.BANK_NAME ");

		log.info("param:[dateFrom:{},dateTo:{},mobank:{}]",dateFrom,dateTo,mobank);
		log.info("sql:"+sql.toString());
		Query query = em.createNativeQuery(sql.toString());
		query.setParameter("dateFrom", dateFrom);
		query.setParameter("dateTo", dateTo);
		query.setParameter("mobank", mobank);
		
		List<NtwfundOutVo> list = EntityUtils.mapping(query.getResultList(), NtwfundOutVo.class, "getNtwfundOutDetailOfMoBank");
		
		return list;
	}
	@Override
	public List<NtwfundOutVo> getNtwfundOutDetailOfCountryOthers_XLS(String[] ct, String dateFrom, String dateTo) {
		String strSql = "";
		for(String str : ct) {
			strSql+=",'"+str+"'";
		}
		StringBuffer sql = new StringBuffer("");
		sql.append(" select distinct c.alt_name,c.lname || ' '|| c.fname eng_name ");
		sql.append(" ,c.acct_no,mb.BANK_NAME ,mb.bank_ccy_code currency ");
		sql.append(" ,p.amount tranc_amt,mb.bank_acct_no bank_acct_no ");
		sql.append(" ,p.cr_date tranc_date,mb.COUNTRY_CODE ,e.info_name ");
		sql.append(" ,c.lname,c.fname ,P.PAY_MTHD ,e.INFO_VALUE GEN_CSV ");
		sql.append(" ,P.AUTH_CODE,e.pmt_id  as TRANC_ID ");
		sql.append(" from dwh_payinfo p, dwh_custinfo c, m_payment_extra_info e,dwh_tcpmbank mb ");
		sql.append(" where p.cr_date >= to_date(:dateFrom,'yyyy-mm-dd hh24:mi:ss') ");
		sql.append(" and p.cr_date <= to_date(:dateTo,'yyyy-mm-dd hh24:mi:ss')");
		sql.append(" and p.acct_id = c.acct_id ");
		sql.append(" and p.payment_sort = 'W' ");
		sql.append(" and p.pmt_id = e.pmt_id(+) ");
		sql.append(" and e.info_name (+)= 'GEN_CSV' ");
		sql.append(" and p.cpm_id=mb.CPM_ID ");
		sql.append(" and not exists (select j.madj_id from dwh_manadj j where j.madj_id = p.cnl_madj_id and j.display = 'N') ");
		sql.append(" and mb.COUNTRY_CODE not in ( " + strSql.substring(1) + ")");
		sql.append(" order by mb.COUNTRY_CODE,p.cr_date,mb.BANK_NAME,c.acct_no,tranc_amt ");

		log.info("param:[dateFrom:{},dateTo:{}]",dateFrom,dateTo);
		log.info("sql:"+sql.toString());
		Query query = em.createNativeQuery(sql.toString());
		query.setParameter("dateFrom", dateFrom);
		query.setParameter("dateTo", dateTo);
		
		List<NtwfundOutVo> list = EntityUtils.mapping(query.getResultList(), NtwfundOutVo.class, "getNtwfundOutDetailOfMoBank");
		
		return list;
	}
	@Override
	public List<NtwfundOutVo> getNtwfundOutDetailOfCountryBankOthers_XLS(String[] mb, String dateFrom, String dateTo) {
		String strSql = "";
		for(String str : mb) {
			strSql+=",'"+str+"'";
		}
		StringBuffer sql = new StringBuffer("");
		sql.append(" select distinct c.alt_name,c.lname || ' '|| c.fname eng_name ");
		sql.append(" ,c.acct_no,mb.BANK_NAME ,mb.bank_ccy_code currency ");
		sql.append(" ,p.amount tranc_amt,mb.bank_acct_no bank_acct_no ");
		sql.append(" ,p.cr_date tranc_date,mb.COUNTRY_CODE ,e.info_name ");
		sql.append(" ,c.lname,c.fname ,P.PAY_MTHD ,e.INFO_VALUE GEN_CSV ");
		sql.append(" ,P.AUTH_CODE,e.pmt_id  as TRANC_ID ");
		sql.append(" from dwh_payinfo p, dwh_custinfo c, m_payment_extra_info e,dwh_tcpmbank mb ");
		sql.append(" where p.cr_date >= to_date(:dateFrom,'yyyy-mm-dd hh24:mi:ss') ");
		sql.append(" and p.cr_date <= to_date(:dateTo,'yyyy-mm-dd hh24:mi:ss')");
		sql.append(" and p.acct_id = c.acct_id ");
		sql.append(" and p.payment_sort = 'W' ");
		sql.append(" and p.pmt_id = e.pmt_id(+) ");
		sql.append(" and e.info_name (+)= 'GEN_CSV' ");
		sql.append(" and p.cpm_id=mb.CPM_ID ");
		sql.append(" and not exists (select j.madj_id from dwh_manadj j where j.madj_id = p.cnl_madj_id and j.display = 'N') ");
		sql.append(" and mb.COUNTRY_CODE='MO' ");
		sql.append(" and mb.COUNTRY_CODE || '-' || mb.BANK_PREFIX  not in ( " + strSql.substring(1) + ")");
		sql.append(" order by c.acct_no,tranc_amt,mb.BANK_NAME ");

		log.info("param:[dateFrom:{},dateTo:{}]",dateFrom,dateTo);
		log.info("sql:"+sql.toString());
		Query query = em.createNativeQuery(sql.toString());
		query.setParameter("dateFrom", dateFrom);
		query.setParameter("dateTo", dateTo);
		
		List<NtwfundOutVo> list = EntityUtils.mapping(query.getResultList(), NtwfundOutVo.class, "getNtwfundOutDetailOfMoBank");
		
		return list;
	}
	@Override
	public List<NtwfundOutVo> getNtwfundOutDetailOfALL_XLS(String dateFrom, String dateTo) {
		StringBuffer sql = new StringBuffer("");
		sql.append(" select distinct c.alt_name,c.lname || ' '|| c.fname eng_name ");
		sql.append(" ,c.acct_no,mb.BANK_NAME ,mb.bank_ccy_code currency ");
		sql.append(" ,p.amount tranc_amt,mb.bank_acct_no bank_acct_no ");
		sql.append(" ,p.cr_date tranc_date,mb.COUNTRY_CODE ,e.info_name ");
		sql.append(" ,c.lname,c.fname ,P.PAY_MTHD ,e.INFO_VALUE GEN_CSV ");
		sql.append(" ,P.AUTH_CODE,e.pmt_id  as TRANC_ID ");
		sql.append(" from dwh_payinfo p, dwh_custinfo c, m_payment_extra_info e,dwh_tcpmbank mb ");
		sql.append(" where p.cr_date >= to_date(:dateFrom,'yyyy-mm-dd hh24:mi:ss') ");
		sql.append(" and p.cr_date <= to_date(:dateTo,'yyyy-mm-dd hh24:mi:ss')");
		sql.append(" and p.acct_id = c.acct_id ");
		sql.append(" and p.payment_sort = 'W' ");
		sql.append(" and p.pmt_id = e.pmt_id(+) ");
		sql.append(" and e.info_name (+)= 'GEN_CSV' ");
		sql.append(" and p.cpm_id=mb.CPM_ID ");
		sql.append(" and not exists (select j.madj_id from dwh_manadj j where j.madj_id = p.cnl_madj_id and j.display = 'N') ");
		sql.append(" order by mb.COUNTRY_CODE,p.cr_date,mb.BANK_NAME,c.acct_no,tranc_amt ");

		log.info("param:[dateFrom:{},dateTo:{}]",dateFrom,dateTo);
		log.info("sql:"+sql.toString());
		Query query = em.createNativeQuery(sql.toString());
		query.setParameter("dateFrom", dateFrom);
		query.setParameter("dateTo", dateTo);
		
		List<NtwfundOutVo> list = EntityUtils.mapping(query.getResultList(), NtwfundOutVo.class, "getNtwfundOutDetailOfMoBank");
		
		return list;
	}


	@Override
	public int getBonuseQueueFlag() {
		String sql = " select * from bonus_queue_flag where type='BIRTHDAY_PRO' and status='A' ";
		
		log.info("sql:"+sql);
		Query query = em.createNativeQuery(sql);
		List list = query.getResultList();
		log.info("BonuseQueueFlag record size:"+list.size());
		
		return list.size();
	}

}
