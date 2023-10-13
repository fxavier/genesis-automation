from django.db import models

class Inquerito(models.Model):
    KEY = models.CharField(max_length=255, primary_key=True)
    data_data_inquerito = models.DateTimeField()
    data_hora_inquerito = models.DateTimeField()
    data_identificacao_agg_familiar_tipo_impacto = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_agg_familiar_nome_proprietario = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_agg_familiar_codigo_igreja = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_agg_familiar_codigo_familia = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_agg_familiar_codigo_machamba = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_agg_familiar_data_nascimento = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_agg_familiar_celular = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_agg_familiar_foto_proprietario = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_agg_familiar_casado = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_agg_familiar_nome_conjugue = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_agg_familiar_data_nasciment_conjugue = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_agg_familiar_foto_conjugue = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_agg_familiar_nome_inquiridor = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_agg_familiar_coordenadas_casa = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_prop_negocio_nome_proprietario_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_prop_negocio_solucao_adoptar = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_prop_negocio_data_nascimento_prop_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_prop_negocio_contacto_prop_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_prop_negocio_doc_identificacao_prop_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_prop_negocio_outro_doc_ident_prop_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_prop_negocio_photo_prop_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_prop_negocio_bairro_vive_prop_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_prop_negocio_bairro_localizacao_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_prop_negocio_numero_senha_localizacao_negocio_gmaps = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_prop_negocio_coordenadas_estabelecimento_comercial = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_prop_negocio_nome_comercial_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_prop_negocio_tipo_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_identificacao_prop_negocio_outro_tipo_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_estrutura_comercial_proprietario_estrutura_comercial = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_estrutura_comercial_como_conseguiu_estrutura_comercial = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_estrutura_comercial_outra_forma_conseguiu_est_comercial = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_estrutura_comercial_tempo_que_possui_estrutura_comercial = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_estrutura_comercial_tem_documento_que_confirma_propriedade_estabelecimento = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_estrutura_comercial_foto_documento_estabelecimento = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_estrutura_comercial_outro_doc_confirma_prop_estabelecimento = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_estrutura_comercial_tipo_material_construcao_estab_comercial_piso = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_estrutura_comercial_outro_tipo_material_est_come_piso = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_estrutura_comercial_tipo_material_construcao_estab_comercial_parede = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_estrutura_comercial_outro_tipo_material_est_come_parede = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_estrutura_comercial_tipo_material_construcao_estab_comercial_tecto = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_estrutura_comercial_outro_tipo_material_construcao_tecto = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_estrutura_comercial_tem_trabalhadores_envolvidos_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_estrutura_comercial_numero_trabalhadores = models.CharField(max_length=255, null=True, blank=True)
    data_renda_proveniente_actividade_comercial_valor_inicial_investido_no_seu_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_renda_proveniente_actividade_comercial_outro_valor_inicial_no_seu_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_renda_proveniente_actividade_comercial_rendimento_bruto_mensal_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_renda_proveniente_actividade_comercial_outro_rendimento_bruto_mensal_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_renda_proveniente_actividade_comercial_lucro_medio_mensal_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_renda_proveniente_actividade_comercial_outro_lucro_medio_mensal_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_outros_meios_substiencia_outras_fontes_renda_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_outros_meios_substiencia_outro_outras_fontes_renda_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_expectativa_compensacao_negocio_compensacao_esperada_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_expectativa_compensacao_negocio_localizacao_estrutura_receber = models.CharField(max_length=255, null=True, blank=True)
    data_expectativa_compensacao_negocio_outra_localizacao_estrutura = models.CharField(max_length=255, null=True, blank=True)
    data_expectativa_compensacao_negocio_outra_compensacao_esperada_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_expectativa_compensacao_negocio_foto_esboco_estrutura_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_expectativa_compensacao_negocio_termo_consentimento_estrutura_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_proprietario_casa = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_tipo_relacao_com_proprietario = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_outro_tipo_posse_casa_outro = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_tipo_posse_casa = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_outro_tipo_posse_casa = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_tipo_doc = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_outro_tipo_doc = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_foto_documento = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_bairro = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_outro_bairro = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_bairro_igreja = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_outro_bairro_igreja = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_bairro_machamba = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_outro_bairro_machamba = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_quarteirao_localizada_familia = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_quarteirao_localizada_machamba = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_outro_quarteirao_localizada_machamba = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_rua_localizada_familia = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_numero_casa_familia = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_referencia_casa = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_referencia_machamba = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_quarteirao_localizada_igreja = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_rua_igreja = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_numero_porta_igreja = models.CharField(max_length=255, null=True, blank=True)
    data_propriedade_da_casa_referencia_igreja = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_lingua_materna = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_outra_lingua = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_numero_de_pessoas_na_familia = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_count = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_tempo_a_familia_vive_no_talhao = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_numero_familias_talhao_terreno = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_razao_familias_viverem_no_mesmo_talhao = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_outra_razao_familias_viverem_mesmo_talhao = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_se_mudar_vao_consigo = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_numero_mulheres_chefe_da_familia_tem = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_todas_esposas_vivem_neste_quintal = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_bairros_onde_vivem_esposas = models.CharField(max_length=255, null=True, blank=True)
    data_patrimonio_renda_consumo_familia_possui_bens = models.CharField(max_length=255, null=True, blank=True)
    data_patrimonio_renda_consumo_bens_que_a_familia_possui = models.CharField(max_length=255, null=True, blank=True)
    data_patrimonio_renda_consumo_numero_de_fontes_de_renda = models.CharField(max_length=255, null=True, blank=True)
    data_patrimonio_renda_consumo_principais_fontes_de_rendas_count = models.CharField(max_length=255, null=True, blank=True)
    data_patrimonio_renda_consumo_principais_fontes_de_rendas = models.CharField(max_length=255, null=True, blank=True)
    data_patrimonio_renda_consumo_renda_media_mensal = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_a_casa_que_vives_e = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_a_igreja_e = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_a_obra_e = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_nome_proprietario_casa_alugada = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_contacto_proprietario = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_outro_tipo_casa = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_outro_tipo_igreja = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_outro_tipo_obra = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_comprimento_talhao = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_largura_talhao = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_de_que_material_feita_vedacao_talhao = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_outro_material_vedacao = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_quantas_estruturas_tem_o_talhao = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_estruturas_talhaos_count = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_estruturas_talhaos = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_quanto_tempo_tem_casa_principal = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_familia_tem_duat_talhao = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_foto_duat = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_familia_possui_outra_estrutura_fora_deste_talhao = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_estrutura_fora_do_talhao = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_localizacao_outra_estrutura = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_outra_localizao_outra_estrutura = models.CharField(max_length=255, null=True, blank=True)
    data_religiao_e_locais_sagrados_religiao = models.CharField(max_length=255, null=True, blank=True)
    data_religiao_e_locais_sagrados_outra_religiao = models.CharField(max_length=255, null=True, blank=True)
    data_religiao_e_locais_sagrados_tempo_chegar_igreja = models.CharField(max_length=255, null=True, blank=True)
    data_religiao_e_locais_sagrados_onde_enterra_entequeridos = models.CharField(max_length=255, null=True, blank=True)
    data_religiao_e_locais_sagrados_outro_lugar_onde_enterra = models.CharField(max_length=255, null=True, blank=True)
    data_religiao_e_locais_sagrados_familia_tem_campas = models.CharField(max_length=255, null=True, blank=True)
    data_religiao_e_locais_sagrados_quantas_campas = models.CharField(max_length=255, null=True, blank=True)
    data_religiao_e_locais_sagrados_frequencia_com_que_vai_cemiterio = models.CharField(max_length=255, null=True, blank=True)
    data_religiao_e_locais_sagrados_outra_frequencia_que_vai_cemiterio = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_machamba_familia = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_tipo_posse = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_outro_tipo_posse = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_forma_aquisicao_machamba = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_valor_da_compra = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_tempo_que_possui_machamba = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_tem_documento_que_confirma_propriedade_terra = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_tipo_de_documento_de_posse_terra = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_outro_tipo_de_documento_de_posse_terra = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_foto_documento_posse_terra = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_quantas_machambas_tem_area = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_onde_estao_localizadas_machambas = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_outro_onde_estao_localizadas_machambas = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_area_da_machamba = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_outros_meios_subsistencia = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_renda_media_mensal_agricultura = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_machamba_possui_estruras = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_numero_estruturas_area_duat = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_Estruturas_area_Duat_count = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_Estruturas_area_Duat = models.CharField(max_length=255, null=True, blank=True)
    data_renda_proveniente_actividades_tipo_produtos_agricolas_produziu_passado = models.CharField(max_length=255, null=True, blank=True)
    data_renda_proveniente_actividades_tem_arvores_de_frutas = models.CharField(max_length=255, null=True, blank=True)
    data_renda_proveniente_actividades_numero_e_idade_de_arvore_de_frutas = models.CharField(max_length=255, null=True, blank=True)
    data_pecuaria_familia_cria_animais = models.CharField(max_length=255, null=True, blank=True)
    data_pecuaria_tipo_animais_cria_repeat = models.CharField(max_length=255, null=True, blank=True)
    data_pecuaria_familia_possui_arvores = models.CharField(max_length=255, null=True, blank=True)
    data_pecuaria_numero_e_idade_arvore_fruta = models.CharField(max_length=255, null=True, blank=True)
    data_pecuaria_utensilios_usados_na_actividade_agricola = models.CharField(max_length=255, null=True, blank=True)
    data_resolucao_conflitos_a_quem_recorre_em_caso_de_conflitos = models.CharField(max_length=255, null=True, blank=True)
    data_resolucao_conflitos_outra_fonte_a_que_recorre = models.CharField(max_length=255, null=True, blank=True)
    data_resolucao_conflitos_metodo_para_receber_informacao = models.CharField(max_length=255, null=True, blank=True)
    data_resolucao_conflitos_outro_metodo_para_receber_informacao = models.CharField(max_length=255, null=True, blank=True)
    data_resolucao_conflitos_metodo_para_dar_informacao = models.CharField(max_length=255, null=True, blank=True)
    data_resolucao_conflitos_outro_metodo_para_dar_informacao = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_donde_busca_agua_para_uso_na_familia = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_outro = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_quantidade_bidoes = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_quantidade_de_metros_cubicos = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_tipo_tratamento_agua = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_outro_tipo_tratamento_agua = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_tempo_levado_para_chegar_local_agua = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_outro_tempo_ao_local_agua = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_como_vai_buscar_agua = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_outra_forma_como_buscar_agua = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_quem_vai_buscar_agua = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_outro_quem_vai_buscar_agua = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_principal_fonte_energia_usada_para_cozinha = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_outra_fonte_energia = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_tempo_para_chegar_local_da_fonte_energia = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_outro_tempo_para_chegar_fonte_energias = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_meio_transporte_para_local_recolha_energia = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_outro_meio_transporte_local_recolha_energia = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_fonte_energia_para_iluminacao_casa = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_outra_fonte_para_iluminacao_casa = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_tempo_para_chegar_local_compra_fonte_energia = models.CharField(max_length=255, null=True, blank=True)
    data_servicos_publicos_outro_tempo_chegar_local_compra_fonte_energia = models.CharField(max_length=255, null=True, blank=True)
    data_educacao_saude_existe_escola_no_bairro = models.CharField(max_length=255, null=True, blank=True)
    data_educacao_saude_tem_membro_estudante = models.CharField(max_length=255, null=True, blank=True)
    data_educacao_saude_quantos_membros = models.CharField(max_length=255, null=True, blank=True)
    data_educacao_saude_escola_que_frequentam_criancas_familia_count = models.CharField(max_length=255, null=True, blank=True)
    data_educacao_saude_escola_que_frequentam_criancas_familia = models.CharField(max_length=255, null=True, blank=True)
    data_educacao_saude_existe_centro_saude = models.CharField(max_length=255, null=True, blank=True)
    data_educacao_saude_nome_centro_saude = models.CharField(max_length=255, null=True, blank=True)
    data_educacao_saude_onde_procura_tratamento = models.CharField(max_length=255, null=True, blank=True)
    data_educacao_saude_outro_local_procura_tratamento = models.CharField(max_length=255, null=True, blank=True)
    data_educacao_saude_tempo_para_chegar_local_tratamento = models.CharField(max_length=255, null=True, blank=True)
    data_educacao_saude_doencas_familia_sofre_mais = models.CharField(max_length=255, null=True, blank=True)
    data_educacao_saude_outra_doenca_que_sofre_familia = models.CharField(max_length=255, null=True, blank=True)
    data_expectativas_tratamento_ao_sair_do_lugar_o_que_espera_como_compensacao = models.CharField(max_length=255, null=True, blank=True)
    data_expectativas_tratamento_outra_compensacao_esperada = models.CharField(max_length=255, null=True, blank=True)
    data_expectativas_tratamento_onde_gostaria_de_ser_reassentado = models.CharField(max_length=255, null=True, blank=True)
    data_expectativas_tratamento_foto_da_machamba = models.CharField(max_length=255, null=True, blank=True)
    data_expectativas_tratamento_foto_esboco_estrutura = models.CharField(max_length=255, null=True, blank=True)
    data_expectativas_tratamento_termo_consentimento = models.CharField(max_length=255, null=True, blank=True)
    data_hora_do_termino = models.CharField(max_length=255, null=True, blank=True)
    data_observacoes = models.CharField(max_length=255, null=True, blank=True)
    data_meta_instanceID = models.CharField(max_length=255, null=True, blank=True)
   

    def __str__(self) -> str:
        return self.KEY

class PecuariaUtensiliosUsados(models.Model):
    KEY = models.CharField(max_length=255, primary_key=True)
    data_pecuaria_utensilios_usados_na_actividade_agricola_utensilios_usados = models.DateTimeField()
    data_pecuaria_utensilios_usados_na_actividade_agricola_outro_utensilio_usado = models.DateTimeField()
    data_pecuaria_utensilios_usados_na_actividade_agricola_quantidade_utensilio = models.DateTimeField()
    data_pecuaria_utensilios_usados_na_actividade_agricola_como_conseguiu_utensilios = models.DateTimeField()
    data_pecuaria_utensilios_usados_na_actividade_agricola_outra_forma_conseguiu_utensilio = models.DateTimeField()
    PARENT_KEY = models.CharField(max_length=255, blank=True, null=True)
    inquerito = models.ForeignKey(Inquerito, on_delete=models.CASCADE, blank=True, null=True)

    class Meta:
        verbose_name = 'Pecuaria Utensilios Usados'
        verbose_name_plural = 'Pecuaria Utensilios Usados'

class RendaProvenienteActividade(models.Model):
    KEY = models.CharField(max_length=255, primary_key=True)
    data_renda_proveniente_actividades_numero_e_idade_de_arvore_de_frutas_arvore_de_frutas_machamba = models.CharField(max_length=255, null=True, blank=True)
    data_renda_proveniente_actividades_numero_e_idade_de_arvore_de_frutas_outra_arvore_de_fruta_machamba = models.CharField(max_length=255, null=True, blank=True)
    data_renda_proveniente_actividades_numero_e_idade_de_arvore_de_frutas_quantidade_arvores_machamba = models.CharField(max_length=255, null=True, blank=True)
    data_renda_proveniente_actividades_numero_e_idade_de_arvore_de_frutas_idade_arvore = models.CharField(max_length=255, null=True, blank=True)
    data_renda_proveniente_actividades_numero_e_idade_de_arvore_de_frutas_tempo_de_producao = models.CharField(max_length=255, null=True, blank=True)
    PARENT_KEY = models.CharField(max_length=255, blank=True, null=True)
    inquerito = models.ForeignKey(Inquerito, on_delete=models.CASCADE, blank=True, null=True)


    class Meta:
        verbose_name = 'Renda Proveniente Actividade'
        verbose_name_plural = 'Rendas Proveniente Actividade'

class EducacaoSaude(models.Model):
    KEY = models.CharField(max_length=255, primary_key=True)
    data_educacao_saude_escola_que_frequentam_criancas_familia_nome_estudante = models.CharField(max_length=255, null=True, blank=True)
    data_educacao_saude_escola_que_frequentam_criancas_familia_nivel_escolaridade = models.CharField(max_length=255, null=True, blank=True)
    data_educacao_saude_escola_que_frequentam_criancas_familia_nome_instituicao_ensino = models.CharField(max_length=255, null=True, blank=True)
    data_educacao_saude_escola_que_frequentam_criancas_familia_localizacao_instituicao_ensino = models.CharField(max_length=255, null=True, blank=True)
    data_educacao_saude_escola_que_frequentam_criancas_familia_tempo_leva_chegar_escola = models.CharField(max_length=255, null=True, blank=True)
    data_educacao_saude_escola_que_frequentam_criancas_familia_meio_transporte_ir_escola = models.CharField(max_length=255, null=True, blank=True)
    data_educacao_saude_escola_que_frequentam_criancas_familia_outro_meio_transporte_criancas_ir_escola = models.CharField(max_length=255, null=True, blank=True)
    PARENT_KEY = models.CharField(max_length=255, blank=True, null=True)
    inquerito = models.ForeignKey(Inquerito, on_delete=models.CASCADE, blank=True, null=True)


    class Meta:
        verbose_name = 'Educacao e Saude'
        verbose_name_plural = 'Educacoes e Saude'

# class Sheet2(models.Model):
#     data_renda_proveniente_actividades_tipo_produtos_agricolas_produziu_passado_tipo_produtos_agricolas = models.DateTimeField()
#     data_renda_proveniente_actividades_tipo_produtos_agricolas_produziu_passado_outro_tipo_produto_agricola = models.DateTimeField()
#     data_renda_proveniente_actividades_tipo_produtos_agricolas_produziu_passado_quantidade_produtos = models.DateTimeField()
#     data_renda_proveniente_actividades_tipo_produtos_agricolas_produziu_passado_peso_produtos = models.DateTimeField()
#     PARENT_KEY = models.CharField(max_length=255, blank=True, null=True)
#     inquerito = models.ForeignKey(Inquerito, on_delete=models.CASCADE, blank=True, null=True)
#     KEY = models.CharField(max_length=255)

#     class Meta:
#         db_table = 'Sheet2'

class EstruturaHabitacional(models.Model):
    KEY = models.CharField(max_length=255, primary_key=True)
    data_estruturas_habitacionais_medidas_esboco_estrutura_codigo_estrutura_esboco = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_medidas_esboco_estrutura_comprimento = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_medidas_esboco_estrutura_largura = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_medidas_esboco_estrutura_material_cobertura_estruturas = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_medidas_esboco_estrutura_outro_material_tecto = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_medidas_esboco_estrutura_material_das_paredes = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_medidas_esboco_estrutura_outro_material_paredes = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_medidas_esboco_estrutura_material_de_soalho_das_estruturas = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_medidas_esboco_estrutura_outro_material_soalho = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_medidas_esboco_estrutura_foto_esboco = models.CharField(max_length=255, null=True, blank=True)
    PARENT_KEY = models.CharField(max_length=255, blank=True, null=True)
    inquerito = models.ForeignKey(Inquerito, on_delete=models.CASCADE, blank=True, null=True)
 

    class Meta:
       verbose_name = 'Estrutura Habitacional'
       verbose_name_plural = 'Estruturas Habitacionais'

class AgregadoFamiliar(models.Model):
    KEY = models.CharField(max_length=255, primary_key=True)
    data_caracteristicas_agg_familiar_membros_familia_nome = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_genero_membro_familia = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_opcao_idade = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_idade_anos = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_idade_meses = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_estado_civil = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_outro_estado_civil = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_ocupacao = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_emprego_informal = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_outro_emprego_informal = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_emprego_formal = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_outra_ocupacao = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_grau_parentesco_com_chefe_familia = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_outro_grau_parentesco = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_nivel_educacao = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_doenca_cronica = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_tipo_doenca_cronica = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_outro_tipo_doenca_cronica = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_deficiencia_fisica_mental = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_membros_familia_escola_de_frequencia = models.CharField(max_length=255, null=True, blank=True)
    PARENT_KEY = models.CharField(max_length=255, blank=True, null=True)
    inquerito = models.ForeignKey(Inquerito, on_delete=models.CASCADE, blank=True, null=True)

    class Meta:
        verbose_name = 'Agregado Familiar'
        verbose_name_plural = 'Agregados familiares'

class EstruturaHabitacionalTalhao(models.Model):
    KEY = models.CharField(max_length=255, primary_key=True)
    data_estruturas_habitacionais_estruturas_talhaos_estruturas_talhao = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_estruturas_talhaos_outra_estrutura = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_estruturas_talhaos_estrutura_sera_afectada = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_estruturas_talhaos_codigo_estrutura_afectada = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_estruturas_talhaos_comprimento = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_estruturas_talhaos_largura = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_estruturas_talhaos_numero_divisoes = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_estruturas_talhaos_nivel_afectacao_estrutura = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_estruturas_talhaos_material_cobertura_estruturas = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_estruturas_talhaos_outro_material_tecto = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_estruturas_talhaos_material_das_paredes = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_estruturas_talhaos_outro_material_paredes = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_estruturas_talhaos_material_de_soalho_das_estruturas = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_estruturas_talhaos_outro_material_soalho = models.CharField(max_length=255, null=True, blank=True)
    data_estruturas_habitacionais_estruturas_talhaos_foto_esboco = models.CharField(max_length=255, null=True, blank=True)
    PARENT_KEY = models.CharField(max_length=255, blank=True, null=True)
    inquerito = models.ForeignKey(Inquerito, on_delete=models.CASCADE, blank=True, null=True)
  
   

    class Meta:
        verbose_name = 'Estrutura Habitacional Talhao'
        verbose_name_plural = 'Estruturas Habitacionais Talhoes'

class FonteDeRenda(models.Model):
    KEY = models.CharField(max_length=255, primary_key=True)
    data_patrimonio_renda_consumo_principais_fontes_de_rendas_principais_fontes_de_renda = models.CharField(max_length=255, null=True, blank=True)
    data_patrimonio_renda_consumo_principais_fontes_de_rendas_outra_fonte_de_renda = models.CharField(max_length=255, null=True, blank=True)
    data_patrimonio_renda_consumo_principais_fontes_de_rendas_fonte_de_renda_emprego_formal = models.CharField(max_length=255, null=True, blank=True)
    PARENT_KEY = models.CharField(max_length=255, blank=True, null=True)
    inquerito = models.ForeignKey(Inquerito, on_delete=models.CASCADE, blank=True, null=True)


    class Meta:
        verbose_name = 'Fonte de Renda'
        verbose_name_plural = 'Fontes de Renda'

class PecuariaArvore(models.Model):
    KEY = models.CharField(max_length=255, primary_key=True)
    data_pecuaria_numero_e_idade_arvore_fruta_arvore_de_frutas = models.CharField(max_length=255, null=True, blank=True)
    data_pecuaria_numero_e_idade_arvore_fruta_outra_arvore_de_fruta = models.CharField(max_length=255, null=True, blank=True)
    data_pecuaria_numero_e_idade_arvore_fruta_quantidade_arvores = models.CharField(max_length=255, null=True, blank=True)
    data_pecuaria_numero_e_idade_arvore_fruta_idade_arvores = models.CharField(max_length=255, null=True, blank=True)
    data_pecuaria_numero_e_idade_arvore_fruta_tempo_producao_frutas = models.CharField(max_length=255, null=True, blank=True)
    PARENT_KEY = models.CharField(max_length=255, blank=True, null=True)
    inquerito = models.ForeignKey(Inquerito, on_delete=models.CASCADE, blank=True, null=True)


    class Meta:
        verbose_name = 'Pecuaria Arvore'
        verbose_name_plural = 'Pecuaria Arvores'

class PatrimonioBens(models.Model):
    KEY = models.CharField(max_length=255, primary_key=True)
    data_patrimonio_renda_consumo_bens_que_a_familia_possui_bens = models.CharField(max_length=255, null=True, blank=True)
    data_patrimonio_renda_consumo_bens_que_a_familia_possui_outro_bem = models.CharField(max_length=255, null=True, blank=True)
    data_patrimonio_renda_consumo_bens_que_a_familia_possui_quantidade = models.CharField(max_length=255, null=True, blank=True)
    PARENT_KEY = models.CharField(max_length=255, blank=True, null=True)
    inquerito = models.ForeignKey(Inquerito, on_delete=models.CASCADE, blank=True, null=True)

    class Meta:
        verbose_name = 'Patrimonio Bens'
        verbose_name_plural = 'Patrimonio Bens'

class LocalViveEsposas(models.Model):
    KEY = models.CharField(max_length=255, primary_key=True)
    data_caracteristicas_agg_familiar_bairros_onde_vivem_esposas_nome_esposa = models.CharField(max_length=255, null=True, blank=True)
    data_caracteristicas_agg_familiar_bairros_onde_vivem_esposas_bairro_onde_vive_esposa = models.CharField(max_length=255, null=True, blank=True)
    PARENT_KEY = models.CharField(max_length=255, blank=True, null=True)
    inquerito = models.ForeignKey(Inquerito, on_delete=models.CASCADE, blank=True, null=True)


    class Meta:
        verbose_name = 'Local onde Vive esposas'
        verbose_name_plural = 'Locais onde Vivem esposas'

class OutrosMeiosSubstiencia(models.Model):
    KEY = models.CharField(max_length=255, primary_key=True)
    data_outros_meios_substiencia_outras_fontes_renda_familia_outras_fontes_renda_negocio = models.CharField(max_length=255, null=True, blank=True)
    data_outros_meios_substiencia_outras_fontes_renda_familia_outro_outras_fontes_renda_negocio = models.CharField(max_length=255, null=True, blank=True)
    PARENT_KEY = models.CharField(max_length=255, blank=True, null=True)
    inquerito = models.ForeignKey(Inquerito, on_delete=models.CASCADE, blank=True, null=True)
    

    class Meta:
        verbose_name = 'Outros Meios de Subsistencia'
        verbose_name_plural = 'Outros Meios de Subsistencia'

class PecuariaTipoAnimaisCria(models.Model):
    KEY = models.CharField(max_length=255, primary_key=True)
    data_pecuaria_tipo_animais_cria_repeat_tipo_animais_familia_cria = models.CharField(max_length=255, null=True, blank=True)
    data_pecuaria_tipo_animais_cria_repeat_outro_tipo_animais_cria = models.CharField(max_length=255, null=True, blank=True)
    data_pecuaria_tipo_animais_cria_repeat_quantidade_animais = models.CharField(max_length=255, null=True, blank=True)
    data_pecuaria_tipo_animais_cria_repeat_vende_os = models.CharField(max_length=255, null=True, blank=True)
    data_pecuaria_tipo_animais_cria_repeat_estrutura_para_albergar_animais = models.CharField(max_length=255, null=True, blank=True)
    data_pecuaria_tipo_animais_cria_repeat_outra_estrutura_para_animais = models.CharField(max_length=255, null=True, blank=True)
    data_pecuaria_tipo_animais_cria_repeat_comprimento_estrutura = models.CharField(max_length=255, null=True, blank=True)
    data_pecuaria_tipo_animais_cria_repeat_largura_estrutura = models.CharField(max_length=255, null=True, blank=True)
    data_pecuaria_tipo_animais_cria_repeat_foto_estrutura_animal = models.CharField(max_length=255, null=True, blank=True)
    PARENT_KEY = models.CharField(max_length=255, blank=True, null=True)
    inquerito = models.ForeignKey(Inquerito, on_delete=models.CASCADE, blank=True, null=True)
    

    class Meta:
        verbose_name = 'Pecuaria Tipo de Animais que cria'
        verbose_name_plural = 'Pecuaria Tipo de Animais que cria'

class EstruturasAreaMachamba(models.Model):
    KEY = models.CharField(max_length=255, primary_key=True)
    data_posse_terra_Estruturas_area_Duat_estrutura_na_area_duat = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_Estruturas_area_Duat_outra_estrutua_area_duat = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_Estruturas_area_Duat_codigo_estrutura_area_duat = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_Estruturas_area_Duat_quantidade_estrutura_area_duat = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_Estruturas_area_Duat_numero_divisoes_estrutura_area_duat = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_Estruturas_area_Duat_comprimento_estrutura_duat = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_Estruturas_area_Duat_largura_estrutura_duat = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_Estruturas_area_Duat_medidas_estruturas_area_duat_esboco_material_das_paredes_estrutura_area_duat = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_Estruturas_area_Duat_medidas_estruturas_area_duat_esboco_outro_material_paredes_estrutura_area_duat = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_Estruturas_area_Duat_medidas_estruturas_area_duat_esboco_material_cobertura_estruturas_area_duat = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_Estruturas_area_Duat_medidas_estruturas_area_duat_esboco_outro_material_cobertura_estrutura_area_duat = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_Estruturas_area_Duat_medidas_estruturas_area_duat_esboco_material_de_soalho_das_estruturas_area_duat = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_Estruturas_area_Duat_medidas_estruturas_area_duat_esboco_outro_material_soalho_estrutura_area_duat = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_Estruturas_area_Duat_medidas_estruturas_area_duat_esboco_foto_estrutura_area_duat = models.CharField(max_length=255, null=True, blank=True)
    PARENT_KEY = models.CharField(max_length=255, blank=True, null=True)
    inquerito = models.ForeignKey(Inquerito, on_delete=models.CASCADE, blank=True, null=True)
    

    class Meta:
        verbose_name = 'Estruturas na area da Machamba'
        verbose_name_plural = 'Estruturas na area da Machamba'

class PrincipaisFontesRendaAgricultura(models.Model):
    KEY = models.CharField(max_length=255, primary_key=True)
    data_posse_terra_outros_meios_subsistencia_principais_fontes_renda_agricultura = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_outros_meios_subsistencia_nome_emprego_formal = models.CharField(max_length=255, null=True, blank=True)
    data_posse_terra_outros_meios_subsistencia_outro_meio_subsistencia = models.CharField(max_length=255, null=True, blank=True)
    PARENT_KEY = models.CharField(max_length=255, blank=True, null=True)
    inquerito = models.ForeignKey(Inquerito, on_delete=models.CASCADE, blank=True, null=True)
    

    class Meta:
        verbose_name = 'Principais Fontes de Renda na Agricultura'
        verbose_name_plural = 'Principais Fontes de Renda na Agricultura'


