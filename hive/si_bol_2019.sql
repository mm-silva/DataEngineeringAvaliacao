USE trabalhoFinal;

CREATE EXTERNAL TABLE IF NOT EXISTS si_bol_2019(
	n_boletim				String,
	data_hora_boletim		String,
	data_inclusao			String,
	tipo_acidente			String,
	desc_tipo_acidente		String,
	cod_tempo				Integer,
	desc_tempo				String,
	cod_pavimento			Integer,
	pavimento				String,
	cod_regional			Integer,
	desc_regional			String,
	origem_boletim			String,
	local_sinalisado		String,
	velocidade_permitida	Integer,
	coordenada_x			Float,
	coordenada_y			Float,
	hora_informada			String,
	indicador_fatalidade	String,
	valor_ups				Integer,
	descricao_upa			String,
	data_alteracao_smsa		String,
	valor_ups_antiga		Integer,
	descricao_ups_antiga	String
)
COMMENT 'Relação de ocorrências de trânsito'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/user/vagrant/atividades/si-bol-2019';

LOAD DATA LOCAL INPATH '/vagrant/aula/csv/si-bol-2019.csv' OVERWRITE INTO TABLE si_bol_2019;