# 1.0.dev305.4

from abc import ABC
from dataclasses import dataclass
from difflib import SequenceMatcher

import requests
from datetime import datetime
import pandas as pd

# from app.loggers import logger_p          # Import
import logging                              # Declare
logger_p = logging.getLogger()              # Declare
logger_p.setLevel(logging.DEBUG)            # Declare

class BaseDataAdapter(ABC):
    """The Base Data Adapter class.

    The Base functionalities includes adapting data from custom sources or models to Astragale schemas.
    It contains some of the mapping schemas encountered while federating services around Astragale and frame the 
    mediation work of external sources. This module subclasses can be setup as a standalone services for data adaptation/mediation.
    (Future) Basic use of this class does not need much more of a `dict-of-dict` for custom mapping, further services or database (sql for instance)
    may need more robust data structures for mapping.

    See :class:`AioliProjectAdapter` for an exemple using a built-in subclass.

    ## On Record Builders:
    Each adapter gets a normalized interface with Astragale Schemas, called `records` for ETL. We differentiate properties as follow:
    + Static: a property that does not change with the adapter's output. *e.g. Aioli service will always photogrammetry point clouds)
    + Dynamic:
        + Internal: information available in a data field at the adapter
        + External: information within the scope but not documented within the adapters (e.g. Aioli schema with only informal datas)
    + Foreign: Information out of the adapter scope, typically URIs or IDs specific to a service/schema

    The `resource_uri` in the resulting record shall hold the initial location of the file. Rationale: choosing between embedding and linking
    a file a decision to be made upstream, not in this layer.

    Args:
        ABC (_type_): _description_
    """
    mapping_schemas = {
        "aioli-NDP_DIAG_MAC": {
            "feature_label" : "Commentaires"
        },
        "aioli-NDP_AstraDIAG_MAC" : {
            "feature_label" : "commentaires"
        }
    }

    def build_record_geometry():
        pass

    def build_record_annotation():
        pass



@dataclass
class AioliLayer: 
    username: str
    layer_id: str
    layer_name: str
    annotation_table: pd.DataFrame
    user_fields: pd.DataFrame

class AioliProject: # class AioliProjectAdapter(BaseDataAdapter):
    def __init__(self, aioli_username: str, aioli_project_id: str, load_layers: bool = False ) -> None:
        self.username = aioli_username
        self.project_id = aioli_project_id

        layers_informations = self._get_annotation_layers_informations()
        if not isinstance(layers_informations, pd.DataFrame):
            raise ValueError(f"aioli_username={aioli_username} to project={aioli_project_id} does not correspond to any registered project in the aioli service, can't build an interface to the project.")
       
        self.layers_information = layers_informations
        self.layers = None
        if load_layers:
            self.layers = self._get_annotation_layers()


    @property
    def micmac_survey_url(self) -> str | None:
        url = f"https://absinthe.aioli.map.cnrs.fr/workspace/usr/{self.username}/projects/{self.project_id}/chantier/MicMac/C3DC_MicMac.ply"
        response_survey_head = requests.head(url=url, verify=False)
        if 'Content-length' in response_survey_head.headers.keys(): # No 404 in Aioli, but it justs returns no bytes sometimes, thats still 200 !. So we check the headers instead ¯\_(ツ)_/¯  
            return url
        else:
            raise ValueError(f"No micmac survey found at url={url} for aioli_username={self.username} to project={self.project_id}. Verify validity of the credential and pids at Aioli's (and the filesystem too, a survey shall exist for a valid registered Aioli project)")
    
    @property
    def layers_ids(self) -> list[str]:
        return self.layers_information['id'].tolist()
    
    @property
    def layers_names(self) -> list[str]:
        return self.layers_information['name'].tolist()
    
    
    # From BaseDataAdapter
    ## builders
    def build_record_geometry(self, builtwork_uri: str, scrs_label: str) -> dict:
        record_photogrammetry_gameaps = {
            # STATIC
                "event_type_uri": "https://astragale.cnrs.fr/th/astra-actdiag/photogrammetry",
                "file_format": ".ply",
            # DYNAMIC INTERNAL
                "file_label": f"aioli_{self.project_id}-C3DC_MicMac.ply",
                "file_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "file_creator": "TODO",
                "resource_uri": self.micmac_survey_url,
            # DYNAMIC EXTERNAL
                "scrs_label": scrs_label,
            # ASTRAGALE
                "builtwork_uri": builtwork_uri

        }
        return record_photogrammetry_gameaps

    def build_record_SurveyLayer(self, layer_id:str,  builtwork_uri: str, geom_uri: str):
        layer_informations = self.get_layer_information_table(layer_id=layer_id)
        layer_label = f"aioli_{self.project_id}-layer_{layer_id}-{layer_informations['name'].iloc[0]}-informations"
        record_aioli_layer_alag = {
            # STATIC
                "annotation_tool_type_uri": "https://astragale.cnrs.fr/th/astra-actdiag/annotation_aioli",
                "file_format": ".json",
            # DYNAMIC INTERNAL
                "annotation_layer_label": layer_label,
                "file_label": f"{layer_label}.json",
                "resource_uri": f"s3://astra-anno-aioli/{self.project_id}/{layer_label}.json",
                "file_creator": layer_informations['owner'].iloc[0],
                "file_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            # DYNAMIC EXTERNAL
            # ASTRAGALE
                "builtwork_uri": builtwork_uri,
                "geom_uri": geom_uri

        }
        return record_aioli_layer_alag, layer_informations.to_json()
    
    def build_record_annotation(self, 
                                schema: str, 
                                layer_id: str,
                                builtwork_uri: str,
                                geom_uri: str,
                                surveyLayer_uri: str,
                                annotation_type: str = 'alteration',
                                ):
        # méthode d'une intelligence limitée pour l'instant. Mais elle fait ce qu'il faut.

        layer_informations = self.get_layer_information_table(layer_id=layer_id)
        # Building record from layer informations, all `None` values will be filled with the annotation informations depending on the schema.
        record_aioli_annotation_gafaaltil = {
            # STATIC
                "observation_type_uri": "http://astragale.cnrs.fr/th/tads/releve_alterations",
                "file_format": ".ply",
            # DYNAMIC INTERNAL
                "resource_uri": None, # f"s3://astra-anno-aioli/{self.project_id}/SCHEMAPLY",
                "file_creator": layer_informations['owner'].iloc[0],
                "file_label": None,
                "feature_label": None,
                "file_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            # DYNAMIC EXTERNAL
                "feature_type_uri": None,
            # ASTRAGALE
                "builtwork_uri": builtwork_uri,
                "geom_uri": geom_uri,
                "annotationLayer_uri": surveyLayer_uri,

        }

        # Get the table with annotations information and `.ply` file location
        layer_annotation_table = self.get_layer_annotation_table(layer_id=layer_id)
        logger_p.info(f"Building record for annotation layer_id={layer_id} ({len(layer_annotation_table)} annotations) with schema={schema}")

        match schema:
            case 'NDP_DIAG_MAC':  
                def record_builder_ndp_diag_mac(row: pd.Series, base_record: dict):
                    def formal_from_informal_label_seqmat(s: str) -> str | None:
                        dict_corr = {
                            "fissure": "https://frollo.notre-dame.science/opentheso/th21/crack",
                            # "Dessalement et ragréage à réaliser sur parement uni ou mouluré" : None,
                            "Pierre altérée à remplacer.": "https://frollo.notre-dame.science/opentheso/th21/alteration"
                        }
                        for k,v in dict_corr.items():
                            seqmat = SequenceMatcher(isjunk=None, a=s, b=k)
                            logger_p.debug(f"({seqmat.ratio()}) a={s}")
                            if seqmat.ratio() > 0.6: # See python docs for 0.6 as arbitrary value
                                return v    
                        return None

                    formal_label = formal_from_informal_label_seqmat(row['Commentaires'])
                    if formal_label is None:
                        logger_p.warning(f"Current informal label found in 'Commentaires' label='{row['Commentaires']}' isnt mapped. Not a valid annotation.")
                        return None                    
                    
                    annotation_infos_record = {
                        "file_label": f"{row['annotation_id']}-Prop.ply",
                        "resource_uri": row['annotation_ply_url'],
                        "feature_label": row['Commentaires'],
                        "feature_type_uri": formal_label
                    }
                    full_record = base_record | annotation_infos_record
                    return full_record

                
                layer_annotation_table['record'] = layer_annotation_table.apply(lambda row: record_builder_ndp_diag_mac(row=row,base_record=record_aioli_annotation_gafaaltil), axis=1)
                records = layer_annotation_table['record'].to_list()
                logger_p.info(f"{(len(records)-records.count(None))}/{len(records)} annotations loaded from layer={layer_id} in project={self.project_id}")

                return records
            
            case 'NDP_AstraDIAG_MAC':
                # Tweaked version of Notre Dame Masonry diagnostic by Roxane Roussel, the ICOMOS thesaurus is mentioned when possible
                
                return record_aioli_annotation_gafaaltil
            case _:
                raise ValueError(f"No schema or mapping for schema={schema} in AioliAdapter. Can't build records for annotations")



    # intrinsic
    def get_layer_information_table(self, layer_id:str) -> pd.DataFrame:
        if layer_id not in self.layers_ids:
            raise ValueError(f"No information table for layer_id={layer_id} could be retrieven for username={self.username}/project={self.project_id}. Available layers are: {self.layers_ids}")

        return self.layers_information.query(f"id == '{layer_id}'")

    def get_layer_annotation_table(self, layer_id:str) -> pd.DataFrame:
        if layer_id not in self.layers_ids:
            raise ValueError(f"No annotation table for layer_id={layer_id} could be retrieven for username={self.username}/project={self.project_id}. Available layers are: {self.layers_ids}")
        
        # baaah, utiliser le asdict (je crois) de dataclass pour éviter de boucler
        for alayer in self.layers:
            if alayer.layer_id == layer_id:
                return alayer.annotation_table

    # initialize layers, project information
    def _get_annotation_layers_informations(self) -> pd.DataFrame | None:
        # In Aioli, `groups` are arbitrarily made from owner information. Layers are in these `groups`
        # Thus we ignore the uuids standing for`groups`, because redundancy over the `owner` field
        # Note: Pandas is a bit overkill, for prototyping it eases the interaction with Aioli data structure as a dark mixture of ~dict and duplicates ~ids.
        
        # Get the groups ids
        response_groups = requests.get(
            url=f"https://absinthe.aioli.map.cnrs.fr/_data/get_groups?id={self.project_id}",
            verify=False, # Invalid certificate for the server should disable verification until situation return to normal, shall raise InsecureRequestWarning
        )
        if response_groups.json() == {}:
            logger_p.error(msg=f"No groups nor layer fetched for project_id={self.project_id}. Check username and project id existence in your aioli_host=https://absinthe.aioli.map.cnrs.fr")
            return None
        df_groups = pd.DataFrame.from_dict(data=response_groups.json(), orient='index')
        groups_ids = df_groups["_id"].to_list()
        logger_p.info(f"Project '{self.project_id}': group_count={len(groups_ids)} with group_id={groups_ids}")

        # Concatenate all the layers information from all the groups.
        df_all_layers = pd.DataFrame()
        for group_id in groups_ids:
            response_layers = requests.get(
                url=f"https://absinthe.aioli.map.cnrs.fr/_data/get_layers?id={group_id}",
                verify=False, # Invalid certificate for the server should disable verification until situation return to normal, shall raise InsecureRequestWarning
            )
            df_layer = pd.DataFrame.from_dict(data=response_layers.json(), orient='index')
            df_all_layers = pd.concat([df_all_layers, df_layer])
        return df_all_layers
    
    def _get_annotation_layers(self) -> list[AioliLayer] | None:
        if not isinstance(self.layers_information, pd.DataFrame):
            logger_p.error(msg=f"No groups nor layer fetched for project_id={self.project_id}. Check username and project id existence in your aioli_host='https://absinthe.aioli.map.cnrs.fr' before trying to read any annotations")
            return None

        layers_list = []
        for layer_id, layer_name in zip(self.layers_information['id'].to_list(), self.layers_information['name'].to_list()):
            response_annotations = requests.get(
                url=f"https://absinthe.aioli.map.cnrs.fr/_data/get_annotations?id={layer_id}",
                verify=False, # Invalid certificate for the server should disable verification until situation return to normal, shall raise InsecureRequestWarning
            )
            df_annofull = pd.DataFrame.from_dict(data=response_annotations.json(), orient='index')
            
            # Get and clean user_fields from aioli
            logger_p.info(layer_id)
            columns_names_nested = self.layers_information['user_fields'][self.layers_information['id'] == layer_id]

            logger_p.info(columns_names_nested) 
            columns_names_nested_dict = columns_names_nested.to_list()[0]
            columns_names_df_clean = pd.DataFrame.from_dict(columns_names_nested_dict, orient='index')

            # Get "user data", c'est bourrin mais les niveaux de dict, et les ids inconnus m'ont saoulé. Optimisation possible avec un apply pour le concat, bon courage.
            df_udata = pd.concat(objs=[(pd.DataFrame.from_dict(data=df_annofull['user_data'].iloc[i], orient='index').T ) for i in range(df_annofull['user_data'].shape[0])])
            df_udata = df_udata.rename(columns = columns_names_df_clean['name'].to_dict())
            df_udata = df_udata.reset_index().drop(labels=['index'], axis=1)

            # # df = pd.concat(objs=[])
            df = pd.concat(objs=[df_annofull[['_id', 'owner']].reset_index().drop(labels=['index'], axis=1), df_udata], axis=1)
            df = df.rename(columns={'_id': 'annotation_id'})
            df['annotation_ply_url'] = df.apply(lambda row: "https://absinthe.aioli.map.cnrs.fr/workspace/usr/{user_id}/projects/{project_id}/chantier/propagated/{annotation_id}/Prop.ply"\
                                                .format(user_id='NDP', project_id=self.project_id, annotation_id=row['annotation_id']), axis=1)
            aiolilayer = AioliLayer(username=self.username,
                layer_id=layer_id,
                layer_name=layer_name,
                annotation_table=df,
                user_fields=columns_names_df_clean)
            
            layers_list.append(aiolilayer)
        return layers_list
    
    
