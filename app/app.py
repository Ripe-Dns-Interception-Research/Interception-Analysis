import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import os

app = dash.Dash(__name__)
country_list = sorted([f.replace('.png', '') for f in os.listdir("results/changepoints") if f.endswith(".png")])

app.layout = html.Div([
    dcc.Dropdown(id='country-dropdown', options=[{'label': c, 'value': c} for c in country_list], value=country_list[0]),
    html.Img(id='plot', style={'width': '100%', 'maxWidth': '1000px'})
])

@app.callback(
    Output('plot', 'src'),
    Input('country-dropdown', 'value')
)
def update_image(country):
    with open(f'results/changepoints/{country}.png', 'rb') as f:
        return "data:image/png;base64," + f.read().encode("base64").decode()

if __name__ == "__main__":
    app.run_server(debug=True, host='0.0.0.0')
