package com.hsjry.zeus.scm.service.impl;

import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.hsjry.zeus.common.context.ThreadContextUtil;
import com.hsjry.zeus.common.exception.BusinessException;
import com.hsjry.zeus.common.model.OperatorInfo;
import com.hsjry.zeus.common.response.BaseResponse;
import com.hsjry.zeus.common.utils.discard.DateUtil;
import com.hsjry.zeus.scm.dao.PayConfirmExtMapper;
import com.hsjry.zeus.scm.dao.PayConfirmMapper;
import com.hsjry.zeus.scm.dao.ShipmentBaseMapper;
import com.hsjry.zeus.scm.dao.SignProtocolMapper;
import com.hsjry.zeus.scm.dto.PayConfirmListDto;
import com.hsjry.zeus.scm.entity.*;
import com.hsjry.zeus.scm.exception.ScmExceptionCode;
import com.hsjry.zeus.scm.request.PayConfirmPageFaceRequest;
import com.hsjry.zeus.scm.request.SignCommonDownLoadRequest;
import com.hsjry.zeus.scm.response.PayConfirmPageFaceResponse;
import com.hsjry.zeus.scm.response.SignFileAddressResponse;
import com.hsjry.zeus.scm.service.PayConfirmFaceService;
import com.hsjry.zeus.scm.state.BillModeEnum;
import com.hsjry.zeus.scm.state.FinanceStatusEnum;
import com.hsjry.zeus.scm.state.SignTypeTemplateEnum;
import com.hsjry.zeus.scm.utils.FileStreamFromSftp;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/*import com.hsjry.lang.common.utils.DateUtil;
import com.hsjry.lang.common.utils.StringUtil;*/

/**
 * @author chenyc<chenyc @ yunrong.cn>
 * @version: $Id: PayConfirmFaceServiceImpl, v1.0  2019/2/26 21:36 asus Exp $$
 * @since 1.0
 */
@Service
public class PayConfirmFaceServiceImpl extends FinanceBaseServiceImpl implements PayConfirmFaceService {

    Logger logger = LoggerFactory.getLogger(PayConfirmFaceServiceImpl.class);


    @Resource
    private PayConfirmExtMapper payConfirmExtMapper;
    @Resource
    private PayConfirmMapper payConfirmMapper;
    @Resource
    private SignProtocolMapper signProtocolMapper;
    @Resource
    private FileStreamFromSftp fileAddress;

    @Resource
    private ShipmentBaseMapper shipmentBaseMapper;

    /**
     * 签署贷款收到确认函-条件分页查询收款确认记录
     *
     * @param request
     * @return
     */
    @Override
    public BaseResponse<PayConfirmPageFaceResponse> queryPayConfirmPage(PayConfirmPageFaceRequest request) {
        PayConfirmPageFaceResponse response = new PayConfirmPageFaceResponse();
        // 组装查询条件
        PayConfirmExample example = new PayConfirmExample();
        this.assemblePayConfirmExample(example, request);
        // 查询收款确认记录
        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        List<PayConfirmListDto> dtoList = this.payConfirmExtMapper.selectByExample(example);
        // 统计收款确认的金额
        List<PayConfirm> payConfirms = this.countPayConfirmAmount(example);
        if (!CollectionUtils.isEmpty(payConfirms)) {
            Integer payConfirmSize = payConfirms.size();
            BigDecimal totalAmount = BigDecimal.ZERO;       // 总金额
            BigDecimal totalBillAmount = BigDecimal.ZERO;   // 总票据金额
            BigDecimal totalLoanAmount = BigDecimal.ZERO;   // 总贷款金额
            for (PayConfirm payConfirm : payConfirms) {
                totalAmount = totalAmount.add(payConfirm.getFinanceAmount());
                // 流动资金总额
                if (payConfirm.getBillingMode().compareTo(BillModeEnum.FLOATING_LOAN.getKey()) == 0) {
                    totalLoanAmount = totalLoanAmount.add(payConfirm.getFinanceAmount());// 总贷款金额
                }
                // 银承协议总额
                if (payConfirm.getBillingMode().compareTo(BillModeEnum.ELEC_ACCEPTANCE_BILL.getKey()) == 0) {
                    totalBillAmount = totalBillAmount.add(payConfirm.getFinanceAmount());// 总票据金额
                }
            }
            response.setTotal(payConfirmSize);
            response.setTotalAmount(totalAmount);
            response.setTotalBillAmount(totalBillAmount);
            response.setTotalLoanAmount(totalLoanAmount);
        }
        PageInfo<PayConfirmListDto> pageInfo = new PageInfo<>(dtoList);
        response.setPayConfirmFaceDTOPageInfo(pageInfo);

        return new BaseResponse<>(response);
    }

    /**
     * 组装查询条件
     *
     * @param example
     * @param request
     */
    private void assemblePayConfirmExample(PayConfirmExample example, PayConfirmPageFaceRequest request) {
        PayConfirmExample.Criteria criteria = example.createCriteria();
        criteria.andFinanceStatusEqualTo(FinanceStatusEnum.LOAN_SUCCESS.getKey());// 放款成功
        criteria.andSupplierIdEqualTo(ThreadContextUtil.getCurrentCustomerId());
        if (StringUtils.isNotEmpty(request.getCustomerName())) {
            criteria.andCustomerNameLike(request.getCustomerName() + "%");
        }
        if (Objects.nonNull(request.getPayDate())) {// 支付日期--用创建时间
            criteria.andCreateTimeBetween(DateUtil.getDayBegin(request.getPayDate()), DateUtil.getDayEnd(request.getPayDate()));
        }
        if (Objects.nonNull(request.getConfirmStatus())) {
            criteria.andConfirmStatusEqualTo(request.getConfirmStatus());
        }

        example.setOrderByClause("record_date desc");
    }

    /**
     * 公共下载签章文件，返回文件路径
     *
     * @param downloadRequest
     * @return
     */
    @Override
    public SignFileAddressResponse signCommonDownLoad(SignCommonDownLoadRequest downloadRequest) {
        checkDownloadAuth(downloadRequest.getBizId(), downloadRequest.getProtocolType());
        SignFileAddressResponse response = new SignFileAddressResponse();
        SignProtocolExample example = new SignProtocolExample();
        //通过业务ID和协议类型查询文件路径
        example.createCriteria().andBizIdEqualTo(downloadRequest.getBizId()).andProtocolTypeEqualTo(downloadRequest.getProtocolType());
        List<SignProtocol> signProtocols = signProtocolMapper.selectByExample(example);
        if (signProtocols != null && signProtocols.size() > 0) {
            String fileAddress = signProtocols.get(0).getFileAddress();
            response.setSignFileAddress(fileAddress);
            response.setSignFileName(fileAddress);//这个字段用于下载文件
        }
        return response;
    }

    @Override
    public byte[] previewPdfFile(String signTemplateUrl) {
        if (StringUtils.isEmpty(signTemplateUrl)) {
            throw new BusinessException(ScmExceptionCode.SIGN_URLISNULL);
        }
        String signFileUrl = signTemplateUrl.substring(0, signTemplateUrl.lastIndexOf("/"));
        String signFileName = signTemplateUrl.substring(signTemplateUrl.lastIndexOf("/") + 1);
        byte[] fileByte = fileAddress.sshSftp(signFileUrl, signFileName);
        if (null != fileByte && fileByte.length != 0) {
            return fileByte;
        }
        return null;
    }

    /**
     * 统计收款确认的金额
     *
     * @param example
     * @return
     */
    private List<PayConfirm> countPayConfirmAmount(PayConfirmExample example) {
        List<PayConfirm> payConfirmList = new ArrayList<PayConfirm>();
        if (example != null) {
            payConfirmList = payConfirmMapper.selectByExample(example);
        }
        return payConfirmList;
    }

    /**
     * 校验下载文件操作权限
     */

    private void checkDownloadAuth(Long bizId, String protocolType) {

        List<String> users = new ArrayList<>();

        switch (SignTypeTemplateEnum.locateByCode(protocolType)) {
            case BANK_ACCEPTANCE: // 保兑仓 银承协议
                FinanceBase financeBase = financeBaseMapper.selectByPrimaryKey(bizId);
                if (Objects.isNull(financeBase)){
                    throw new BusinessException(ScmExceptionCode.FINANCE_APPLY_EXCEPTION);
                }
                users.add(financeBase.getCustomerId());
                users.add(financeBase.getNetworkSubjectId());
                checkParams(users, financeBase.getUserId());
                break;
            case PAYMENT_CONFIRM: // 保兑仓 货款收到确认
                PayConfirm payConfirm = payConfirmMapper.selectByPrimaryKey(bizId);
                if (Objects.isNull(payConfirm)){
                    throw new BusinessException(ScmExceptionCode.PAY_COMFIRM_IS_EMPTY);
                }
                FinanceBase financeBase1 = financeBaseMapper.selectByPrimaryKey(payConfirm.getFinanceApplyId());
                users.add(financeBase1.getCustomerId());
                users.add(financeBase1.getNetworkSubjectId());
                checkParams(users, financeBase1.getUserId());
                break;
            case STORAGE_GOODS_APPLY_CHART: // 保兑仓 提货申请
                RedeemBase redeemBase = redeemBaseMapper.selectByPrimaryKey(bizId);
                if (Objects.isNull(redeemBase)){
                    throw new BusinessException(ScmExceptionCode.REDEEM_IS_NULL);
                }
                users.add(redeemBase.getCustomerId());
                users.add(redeemBase.getNetworkSubjectId());
                checkParams(users, redeemBase.getUserId());
                break;
            case STORAGE_GOODS_ADVICE_NOTE: // 保兑仓 发货通知
                ShipmentBase shipmentBase = shipmentBaseMapper.selectByPrimaryKey(bizId);
                if (Objects.isNull(shipmentBase)){
                    throw new BusinessException(ScmExceptionCode.SHIPMENT_IS_NULL);
                }
                users.add(shipmentBase.getCustomerId());
                users.add(shipmentBase.getNetworkSubjectId());
                checkParams(users, shipmentBase.getUserId());
                break;
            case STORAGE_GOODS_ADVICENOTE_CONFIRM: // 保兑仓 发货通知回执
                ShipmentBase shipmentBase1 = shipmentBaseMapper.selectByPrimaryKey(bizId);
                if (Objects.isNull(shipmentBase1)){
                    throw new BusinessException(ScmExceptionCode.SHIPMENT_IS_NULL);
                }
                users.add(shipmentBase1.getCustomerId());
                users.add(shipmentBase1.getNetworkSubjectId());
                checkParams(users, shipmentBase1.getUserId());
                break;
            case STORAGE_GOODS_RECEIVE_CONFIRM: // 保兑仓 收货确认
                ShipmentBase shipmentBase3 = shipmentBaseMapper.selectByPrimaryKey(bizId);
                if (Objects.isNull(shipmentBase3)){
                    throw new BusinessException(ScmExceptionCode.SHIPMENT_IS_NULL);
                }
                users.add(shipmentBase3.getCustomerId());
                users.add(shipmentBase3.getNetworkSubjectId());
                checkParams(users, shipmentBase3.getUserId());
                break;
            case STORAGE_REFUND_ADVICE_NOTE: // 保兑仓 差额回购
                FinanceBase financeBase2 = financeBaseMapper.selectByPrimaryKey(bizId);
                if (Objects.isNull(financeBase2)){
                    throw new BusinessException(ScmExceptionCode.FINANCE_APPLY_EXCEPTION);
                }
                users.add(financeBase2.getCustomerId());
                users.add(financeBase2.getNetworkSubjectId());
                checkParams(users, financeBase2.getUserId());
                break;
        }
    }

    private void checkParams(List<String> users, Long operatorId) {
        OperatorInfo currentLogin = ThreadContextUtil.getCurrentLogin();
        String loginCustomerId = currentLogin.getCustomerId(); // 门户登陆
        logger.info("门户登陆客户id:::" +loginCustomerId);
        Long loginOperatorId = currentLogin.getOperatorId(); // 资金端登陆
        logger.info("资金端登陆操作员id:::" +loginOperatorId);
        if (loginCustomerId != null){
            if (!users.contains(loginCustomerId)){
                throw new BusinessException(ScmExceptionCode.USER_ID_ERROR);
            }
        }else {
            if (loginOperatorId.longValue() != operatorId.longValue()){
                throw new BusinessException(ScmExceptionCode.USER_ID_ERROR);
            }
        }
    }

}
