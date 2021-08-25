-- Table: hb_bridge_account

-- DROP TABLE hb_bridge_account;

CREATE TABLE hb_bridge_account
(
  "姓名" text,
  "银行卡" text NOT NULL,
  CONSTRAINT "hb_bridge_account_银行卡_key" UNIQUE ("银行卡")
)
WITH (
  OIDS=FALSE
);
ALTER TABLE hb_bridge_account
  OWNER TO postgres;


-- Table: hb_transaction_details

-- DROP TABLE hb_transaction_details;

CREATE TABLE hb_transaction_details
(
  "收钱方" text,
  "出钱方" text,
  "查询账号" text,
  "对方账号姓名" text,
  "对方账号卡号" text,
  "金额" money,
  "余额" money,
  "借贷标志" text,
  "交易类型" text,
  "交易结果" text,
  "交易时间" text,
  "交易开户行" text,
  "交易网点名称" text, 
  "交易流水号" text,
  "凭证号" text, 
  "终端号" text,
  "现金标志" text,
  "交易摘要" text,
  "商户名称" text,
  "ip地址" text,
  "mac地址" text,
  "唯一id" text NOT NULL,
  "人物等级" text,
  CONSTRAINT "hb_transaction_details_唯一id_key" UNIQUE ("唯一id")
)
WITH (
  OIDS=FALSE
);
ALTER TABLE hb_transaction_details
  OWNER TO postgres;
