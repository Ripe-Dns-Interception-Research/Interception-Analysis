import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import os
import base64

app = dash.Dash(__name__)
country_list = sorted([f.replace('.png', '') for f in os.listdir("/root/interceptionInjection/results/changepoints") if f.endswith(".png")])

app.layout = html.Div([
    dcc.Dropdown(id='country-dropdown', options=[{'label': c, 'value': c} for c in country_list], value=country_list[0]),
    html.Img(id='plot', style={'width': '100%', 'maxWidth': '1000px'})
])

@app.callback(
    Output('plot', 'src'),
    Input('country-dropdown', 'value')
)
def update_image(country):
    image_path = os.path.join("results", "changepoints", f"{country}.png")
    with open(image_path, 'rb') as f:
        encoded = base64.b64encode(f.read()).decode('utf-8')
    return f"data:image/png;base64,{encoded}"

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')
