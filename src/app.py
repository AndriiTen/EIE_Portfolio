# from api import app
from ariadne import load_schema_from_path, make_executable_schema, \
    graphql_sync, snake_case_fallback_resolvers, ObjectType
# from ariadne.constants import PLAYGROUND_HTML
from flask import request, jsonify, Flask
from flask_cors import CORS
# from main import resolve_Intraday_API
from main import resolve_EIE
import settings

query = ObjectType("Query")
query.set_field("EIE_Calculator", resolve_EIE)

type_defs = load_schema_from_path("/kns-dta-data-kubernetes-namespace-tst-tst-kct/EIE-Economic_Indicators_Extractor.Bot-API_2.0/schema.graphql")
schema = make_executable_schema(
    type_defs, query, snake_case_fallback_resolvers
)
app = Flask(__name__)
CORS(app)
# schema.type_map['NumpyArray'] = numpy_array_scalar

@app.route("/health", methods=["GET"])
def health():
    return 'OK', 200

@app.route("/graphql", methods=["POST"])
def graphql_server():
    data = request.get_json()
    success, result = graphql_sync(
        schema,
        data,
        context_value=request,
        debug=app.debug
    )
    status_code = 200 if success else 400
    print(jsonify(result))
    return jsonify(result), status_code

if __name__ == '__main__':
    app.run(host=settings.SERVER_HOST,port=settings.SERVER_PORT,debug=True)