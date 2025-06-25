from flask import Blueprint, render_template, request

views = Blueprint('views', __name__, template_folder='../templates')

@views.route('/')
def home():
	return render_template("home.html")

@views.route('/stats')
def stats():
    return render_template("stats.html")

@views.route('/teams')
def teams():
	return render_template("teams.html")	

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
    selected_country = request.args.get('country', 'all')
    
    if selected_country == 'all':
        countries = ['nl', 'be', 'lu']
    else:
        countries = selected_country.split(',')
    
    # Validate countries input
    valid_countries = ['nl', 'be', 'lu']
 
    for country in countries:
        if country not in valid_countries:
            return render_template("error.html", message=f"Invalid country code: {country}")

    # If no specific countries are selected, default to all
    if not countries or countries == ['']:
        countries = ['nl', 'be', 'lu']
    
    from database.db_down import gather_leaderboard
	
    # Gather leaderboard data
    df_players = gather_leaderboard(countries=countries)
    players = df_players.to_dict(orient='records')
    return render_template(
        "leaderboard.html", 
        players=players,
        selected_country=selected_country
    )