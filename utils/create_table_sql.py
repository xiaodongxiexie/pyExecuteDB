# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2020/4/4

# pip install Jinja2

from jinja2 import Template


template_sql = Template("""
                            CREATE TABLE IF NOT EXISTS  {{table_name}}
                            (
                                {% for segment, others in table_detail.items() %}
                                    {% if loop.last %}
                                        {{ segment }} {{ others }}
                                    {% else %}
                                        {{ segment}} {{ others }},
                                    {% endif %}
                                {% endfor %}
                            )
                        """)


mapping = {
    "float":
        {
            "sqlite": "REAL",
        }
}

links_sql = template_sql.render(table_name="links",
                                table_detail={
                                    "movieId": "INTEGER NOT NULL",
                                    "imdbId": "INTEGER NOT NULL",
                                    "tmdbId": "INTEGER",
                                            }
                                )
tags_sql = template_sql.render(table_name="tags",
                               table_detail={
                                   "userId": "INTEGER NOT NULL",
                                   "movieId": "INTEGER NOT NULL",
                                   "tag": "TEXT",
                                   "timestamp": "INTEGER",
                                           }
                               )
movies_sql = template_sql.render(table_name="movies",
                                 table_detail={
                                     "movieId": "INTEGER NOT NULL",
                                     "title": "TEXT",
                                     "genres": "TEXT",
                                           }
                                 )
ratings_sql = template_sql.render(table_name="ratings",
                                  table_detail={
                                      "userId": "INTEGER NOT NULL",
                                      "movieId": "INTEGER NOT NULL",
                                      "rating": mapping["float"]["sqlite"],
                                      "timestamp": "INTEGER",
                                              }
                                  )
