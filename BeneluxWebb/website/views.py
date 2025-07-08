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
    # Parse parameters with robust error handling
    try:
        selected = request.args.getlist('countries')
        # Check if countries is empty or not provided, default to all countries
        if not selected:
            selected = ['nl', 'be', 'lu']
            print("No countries provided, defaulting to all:", selected)
        else:
            print("Countries from request:", selected)
        
        search = request.args.get('search', '').strip()
        min_elo = request.args.get('min_elo', type=int, default=0)
        max_elo = request.args.get('max_elo', type=int, default=5000)
        sort = request.args.get('sort', 'elo_desc')
        page = max(1, request.args.get('page', type=int, default=1))  # Ensure page is at least 1
        per_page = request.args.get('per_page', type=int, default=20)
        
        # Ensure per_page is within reasonable bounds
        per_page = max(10, min(per_page, 100))
        
        # Check if this is an AJAX request
        is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            
        # Get data from the database
        from database.db_down import gather_leaderboard
        df_players = gather_leaderboard(
            countries=selected,
            search=search,
            min_elo=min_elo,
            max_elo=max_elo,
        )
        
        # Apply sorting
        reverse = sort == 'elo_desc'
        df_players = df_players.sort_values(by='faceit_elo', ascending=not reverse)
        
        # Pagination
        total = len(df_players)
        start = (page - 1) * per_page
        end = start + per_page
        paginated_df = df_players.iloc[start:end]
        
        data = paginated_df.to_dict(orient='records')
        
        # Calculate total pages for pagination
        total_pages = ((total - 1) // per_page) + 1 if total > 0 else 1

        if is_ajax:
            return render_template(
                "partials/_leaderboard_table.html",
                data=data,
                sort=sort,
                page=page,
                per_page=per_page,
                total=total,
                total_pages=total_pages
            )
        
        return render_template(
            "leaderboard.html",
            data=data,
            selected=selected,
            search=search,
            min_elo=min_elo,
            max_elo=max_elo,
            sort=sort,
            page=page,
            total=total,
            per_page=per_page,
            total_pages=total_pages
        )
    
    except Exception as e:
        print(f"ERROR in leaderboard route: {e}")
        
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return f"""
            <div class="alert alert-danger">
                <h4 class="alert-heading">Error</h4>
                <p>There was an error processing your request: {e}</p>
                <hr>
                <p class="mb-0">Please try refreshing the page.</p>
            </div>
            """
        
        return render_template(
            "leaderboard.html", 
            data=[],
            selected=['nl', 'be', 'lu'],
            search='',
            min_elo=0,
            max_elo=5000,
            sort='elo_desc',
            page=1,
            total=0,
            per_page=20,
            total_pages=1,
            error=str(e)
        )