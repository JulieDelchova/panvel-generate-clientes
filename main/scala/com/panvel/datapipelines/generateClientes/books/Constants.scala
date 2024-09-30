package com.panvel.datapipelines.generateClientes.books

object Constants {

  //Final Columns
  val CODIGO_CLIENTE: String = "codigo_cliente"
  val DATA_NASCIMENTO: String = "data_nascimento"
  val IDADE: String = "idade"
  val SEXO: String = "sexo"
  val UF: String = "uf"
  val CIDADE: String = "cidade"
  val ESTADO_CIVIL: String = "estado_civil"
  val FLAG_LGPD_CALL: String = "flag_lgpd_call"
  val FLAG_LGPD_SMS: String = "flag_lgpd_sms"
  val FLAG_LGPD_EMAIL: String = "flag_lgpd_email"
  val FLAG_LGPD_PUSH: String = "flag_lgpd_push"

  //Columns from clientes raw table
  val V_ID_CLI: String = "v_id_cli"
  val D_DT_NASC: String = "d_dt_nasc"
  val V_SX_CLI: String = "v_sx_cli"
  val N_EST_CVL: String = "n_est_cvl"

  //Columns from clientes_opt raw table
  val B_PUSH: String = "b_push"
  val B_SMS: String = "b_sms"
  val B_EMAIL: String = "b_email"
  val B_CALL: String = "b_call"

  //Columns from enderecos_clientes raw table
  val V_LCL: String = "v_lcl"
  val V_UF: String = "v_uf"
}