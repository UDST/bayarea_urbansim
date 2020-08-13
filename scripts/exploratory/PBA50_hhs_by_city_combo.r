library(scales)
library(ggplot2)
library(ggthemr)
library(tidyverse)
ggthemr('fresh')


draft_blueprint <-
  read.csv(file.path(
    Sys.getenv('DROPBOX_LOC'),
    'Data/PBA2050/pba50_tothh_combo.csv'))
draft_blueprint$runid<-factor(draft_blueprint$runid)

tail(draft_blueprint)

pdf(
  file.path(
    Sys.getenv('DROPBOX_LOC'),
    sprintf('plots/PBA50/draft_blueprint_combo_juris_s21_w_pba.pdf')
  ),
  width = 17,
  height = 11
)

  for(cnty in levels(draft_blueprint$county)){
  p3 <- ggplot(data = draft_blueprint[draft_blueprint$county==cnty,],
               aes(
                 y = tothh,
                 color = runid,
                 group = runid,
                 x = year
               )) +
    geom_line(size=1.5) +
    geom_point(shape=16,size=2.5,fill="white",color="white")+
    geom_point(shape=1,size=2.5,fill="white")+
    scale_y_continuous(label=comma)+
    scale_fill_brewer(palette = "Set1")+
    theme(axis.text.x = element_text(angle = 0, size = 9),
          legend.position = "bottom") +
    facet_wrap(~juris,scales='free')+
    #guides(col = guide_legend(nrow = 5)) +
    labs(
      title = sprintf(
        "Household growth by city, 2010-2050\n%s County Jurisdictions",cnty),
      subtitle = sprintf('Source: ABAG / MTC, Draft Blueprint Basic UrbanSim, various runids'),
      caption = format(Sys.time(), "%a %b %d %Y %H:%M:%S")
    )
  print(p3)
  }

dev.off()

