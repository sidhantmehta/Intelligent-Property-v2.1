import plotly as py
import plotly.graph_objs as go

#   OUTPUT MODULE START-------
def create_mapbox_all_houses(self, df, color_series, text, color_series_title, min, max):
    mapbox_access_token = 'pk.eyJ1Ijoic2lkaGFudG1laHRhIiwiYSI6ImNqbWFuYjlvZjFhZncza250MjdlYXJpdnYifQ.hCyE1Wyivqlinl6j3-mCzw'

    if min == '':
        min = color_series.min()

    if max =='':
        max = color_series.max()

    data = [
        go.Scattermapbox(
            lat=df.lat,
            lon=df.long,
            mode='markers',
            marker=dict(
                size=14,
                opacity=0.5,
                autocolorscale = False,
                reversescale = True,
                colorscale= 'YlOrRd',
                cmin=min,
                color=color_series,
                cmax=max,
                colorbar=dict(
                    title=color_series_title
                )
            ),
            text=text
        )
    ]

    layout = go.Layout(
        autosize=True,
        hovermode='closest',
        mapbox=dict(
            accesstoken=mapbox_access_token,
            center=dict(
                lon=-0.13,
                lat=51.5
            ),
            bearing=0,
            pitch=0,
            zoom=8
        ),
    )

    fig = dict(data=data, layout=layout)
    py.offline.plot(fig, filename='file.html')
#   OUTPUT MODULE END-------