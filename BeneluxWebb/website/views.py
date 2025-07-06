from flask import Blueprint, render_template, request, redirect, url_for

views = Blueprint('views', __name__, template_folder='../templates')

# Redirect root URL to the ESEA page
@views.route('/')
def home_redirect():
    return redirect(url_for('views.esea'))

@views.route('/stats')
def stats():
    return render_template("stats.html")

@views.route('/esea')
def esea():
	from database.db_down import gather_esea_season_numbers
	season_numbers = gather_esea_season_numbers()
	return render_template("esea.html", season_numbers=season_numbers)

@views.route('/esea/season/<int:season_number>')
def esea_season_partial(season_number):
    from database.db_down import gather_esea_teams_benelux
    esea_data = gather_esea_teams_benelux(szn_number=season_number)
    
    # Extract teams for the specified season
    divisions = esea_data.get(season_number, {})
    
    return render_template('_esea_season.html', divisions=divisions, season=season_number)

@views.route('/leaderboard')
def leaderboard():
    selected = request.args.getlist('countries') or ['nl', 'be', 'lu']  # default to all
    
    from database.db_down import gather_leaderboard
	
    # Gather leaderboard data
    df_players = gather_leaderboard(countries=selected)
    data = df_players.to_dict(orient='records')
    
    return render_template(
        "leaderboard.html", 
        data=data,
        selected=selected
    )