USE trabalhoFinal;

CREATE EXTERNAL TABLE IF NOT EXISTS si_env(
	num_boletim 			String,
	data_hora_boletim		String,
	n_envolvido				Integer,
	condutor				String, 
	cod_severidade			Integer,
	desc_severidade			String,
	sexo					String,
	cinto_seguranca			String,
	Embreagues				String,
	Idade					Integer,
	nascimento				String,
	categoria_habilitacao	String,
	descricao_habilitacao	String,
	declaracao_obito		Integer,
	cod_severidade_antiga	Integer,
	especie_veiculo			String,
	pedestre				String,
	passageiro				String
)
COMMENT 'Relação de pessoas envolvidas em acidentes de trânsito'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/user/vagrant/atividades/si_env-2019';

LOAD DATA LOCAL INPATH '/vagrant/aula/csv/si_env-2019.csv' OVERWRITE INTO TABLE si_env;