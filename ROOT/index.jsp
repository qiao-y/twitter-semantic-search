<%@ page import="edu.columbia.watson.twitter.*" %>
<%@ page import="edu.columbia.watson.twitter.util.*" %>
<%@ page import="java.util.*" %>

<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Semantic Search Over Tweets</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="">
    <meta name="author" content="">

    <!-- Le styles -->
    <link href="css/bootstrap.css" rel="stylesheet">
    <style type="text/css">
      body {
        padding-top: 60px;
        padding-bottom: 40px;
      }
    </style>
    <link href="css/bootstrap-responsive.css" rel="stylesheet">

    <!-- HTML5 shim, for IE6-8 support of HTML5 elements -->
    <!--[if lt IE 9]>
      <script src="js/html5shiv.js"></script>
    <![endif]-->

    <!-- Fav and touch icons -->
    <link rel="apple-touch-icon-precomposed" sizes="144x144" href="ico/apple-touch-icon-144-precomposed.png">
    <link rel="apple-touch-icon-precomposed" sizes="114x114" href="ico/apple-touch-icon-114-precomposed.png">
      <link rel="apple-touch-icon-precomposed" sizes="72x72" href="ico/apple-touch-icon-72-precomposed.png">
                    <link rel="apple-touch-icon-precomposed" href="ico/apple-touch-icon-57-precomposed.png">
                                   <link rel="shortcut icon" href="ico/favicon.png">
		<%
		if (application.getAttribute("SearchMain") == null) {
			application.setAttribute("SearchMain", new SearchMain());
		}
		%>
  </head>

  <body>

    <div class="navbar navbar-inverse navbar-fixed-top">
      <div class="navbar-inner">
        <div class="container">
          <button type="button" class="btn btn-navbar" data-toggle="collapse" data-target=".nav-collapse">
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
          </button>
          <a class="brand" href="index.jsp">Semantic Search Over Tweets</a>
          <div class="nav-collapse collapse">
            <ul class="nav">
              <li class="active"><a href="index.jsp">Home</a></li>
              <li><a href="#about">About</a></li>
              <li><a href="mailto:twitter-semantic-search@cs.columbia.edu">Contact</a></li>
            </ul>
          </div><!--/.nav-collapse -->
        </div>
      </div>
    </div>

    <div class="container">

      <!-- Main hero unit for a primary marketing message or call to action -->
      <div class="hero-unit">
        <h1>Search Over 6,000,000+ Tweets</h1>
        <br/><br/>
				<form action="index.jsp" method="post">
					Query: <input type="text" class="input-block-level" name="query" value="<%= request.getParameter("query") == null ? "2022 FIFA soccer" : request.getParameter("query")%>"/> <br/>
					Linked Tweet ID: <input type="text" class="input-block-level" name="linkedTweet" value="<%= request.getParameter("linkedTweet") == null ? "35048150574039040" : request.getParameter("linkedTweet") %>"> <br/>
					<button type="submit" class="btn btn-large btn-primary">Submit</button>
				</form>
				<% if (request.getParameter("query") == null ||
							request.getParameter("query").trim().length() == 0 ||
							request.getParameter("linkedTweet") == null ||
							request.getParameter("linkedTweet").trim().length() == 0) {%>
							<h3>Please input a <font color="red"> query </font> and a <font color="red"> linked tweet ID </font> </h3>
				<h4>Query: keywords used to search through tweet corpus</h4>
				<h4>Linked Tweet ID: ID of a tweet related to the given query (required by <a href="https://sites.google.com/site/microblogtrack/2012-guidelines" target="_blank"> TREC Microblog 2012 </a>)</h4> <br/>
				<h3>Sample Input: </h3>
				<h4>Query = "Hu Jintao visit to the United States"</h4>
				<h4>Linked Tweet ID = "34695232985645056"</h4>
				<br/>
				<h3>Complete Query List: </h3>
				<h4>Full List of Queries from TREC Microblog 2011 is <a href="query_list_2011.html" target="_blank">here</a></h4>
				<h4>Full List of Queries from TREC Microblog 2012 is <a href="query_list_2012.html" target="_blank">here</a></h4>
				<% } else { 
						List<ReadableResult> results = ((SearchMain)application.getAttribute("SearchMain")).run(Long.parseLong(request.getParameter("linkedTweet")), request.getParameter("query"));
						%>
						<table border="1">
							<tr>
								<th>Tweet ID</th>
								<th>Tweet</th>
								<th>Score</th>
							</tr>
							<% for (ReadableResult result : results) { %>
							<tr>
								<td><%= result.getTweetID() %></td>
								<td><%= result.getTweet() %></td>
								<td><%= result.getScore() %></td>
							</tr>
							<% } %>
						</table>
						<%
				}%> 
      </div>

      <footer>
        <p>&copy; Columbia University, <a href="http://www.columbia.edu/~ag3366/" target="_blank">COMS E6998_9 Spring 2013</a></p>
				<p>Advised by Prof. <a href="http://researcher.watson.ibm.com/researcher/view.php?person=us-gliozzo" target="_blank"> Alfio M. Gliozzo </a> and Or Biran</p>
      </footer>

    </div> <!-- /container -->

    <!-- Le javascript
    ================================================== -->
    <!-- Placed at the end of the document so the pages load faster -->
    <script src="js/jquery.js"></script>
    <script src="js/bootstrap-transition.js"></script>
    <script src="js/bootstrap-alert.js"></script>
    <script src="js/bootstrap-modal.js"></script>
    <script src="js/bootstrap-dropdown.js"></script>
    <script src="js/bootstrap-scrollspy.js"></script>
    <script src="js/bootstrap-tab.js"></script>
    <script src="js/bootstrap-tooltip.js"></script>
    <script src="js/bootstrap-popover.js"></script>
    <script src="js/bootstrap-button.js"></script>
    <script src="js/bootstrap-collapse.js"></script>
    <script src="js/bootstrap-carousel.js"></script>
    <script src="js/bootstrap-typeahead.js"></script>

  </body>
</html>

