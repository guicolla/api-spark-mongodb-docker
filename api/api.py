from flask import Flask
from flask import jsonify
from flask import request
from flask_pymongo import PyMongo

app = Flask(__name__)

app.config['MONGO_DBNAME'] = 'geofusion'
app.config['MONGO_URI'] = 'mongodb://mongo/geofusion'

mongo = PyMongo(app)

@app.route('/', methods=['GET'])
def get_all_information():
  information = mongo.db.calculos
  output = []
  
  for s in information.find():
    try:
       output.append({'codigo' : s['codigo'], 'nome' : s['nome'], 'nome_bairro' : s['nome_bairro'], 'faixa_preco' : s['faixa_preco'], 'populacao' : s['populacao'], 'densidade_demografica' : s['densidade_demografica'], 'weekday' : s['weekday'], 'weekday_count' : s['weekday_count'], 'periodo_manha' : s['periodo_manha'], 'manha_count' : s['manha_count'], 'periodo_tarde' : s['periodo_tarde'], 'tarde_count' : s['tarde_count'], 'periodo_noite' : s['periodo_noite'], 'noite_count' : s['noite_count'], 'endereco' : s['endereco']})
    except KeyError:
       output.append({'codigo' : s['codigo'], 'nome' : s['nome'], 'endereco' : '', 'nome_bairro' : s['nome_bairro'], 'faixa_preco' : s['faixa_preco'], 'populacao' : s['populacao'], 'densidade_demografica' : s['densidade_demografica'], 'weekday' : s['weekday'], 'weekday_count' : s['weekday_count'], 'periodo_manha' : s['periodo_manha'], 'manha_count' : s['manha_count'], 'periodo_tarde' : s['periodo_tarde'], 'tarde_count' : s['tarde_count'], 'periodo_noite' : s['periodo_noite'], 'noite_count' : s['noite_count']})
       return jsonify({'result' : output})

if __name__ == '__main__':
    app.run(host='0.0.0.0')
