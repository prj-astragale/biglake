# 1.0.dev304.0

from pathlib import Path
import logging



# logger_f = logging.getLogger()  # Declare
# logger_f.setLevel(logging.DEBUG)  # Declare

logger_p = logging.getLogger()
logger_p.setLevel(logging.INFO)

import open3d as o3d
import numpy as np


def match_points_to_cloud_rknn(points_candidates, points_sourcecloud, distance_treshold, previsualization=False):
    points_candidates_cloud = o3d.geometry.PointCloud()
    points_candidates_cloud.points = o3d.utility.Vector3dVector(points_candidates)
    logger_p.info(
        f"Loaded: {len(points_candidates_cloud.points)} candidate points, center={points_candidates_cloud.get_center()}"
    )

    points_sourcecloud_cloud = o3d.geometry.PointCloud()
    points_sourcecloud_cloud.points = o3d.utility.Vector3dVector(points_sourcecloud)
    logger_p.info(
        f"Loaded: {len(points_sourcecloud_cloud.points)} pointcloud points, center={points_sourcecloud_cloud.get_center()}"
    )

    kept_indexes = []
    pcdtree_pcd = o3d.geometry.KDTreeFlann(points_sourcecloud_cloud)
    for idx_cen, cen in enumerate(points_candidates_cloud.points):
        [k, idx, dist] = pcdtree_pcd.search_hybrid_vector_3d(
            cen, radius=distance_treshold, max_nn=1
        )
        if k:
            kept_indexes.append(idx_cen)
    logger_p.info(
        f"Matched {len(kept_indexes)}/{len(points_candidates)} points from candidates"
    )
    logger_p.debug(f"Points={kept_indexes}")

    ma_array = np.zeros(len(points_candidates), dtype=bool)
    ma_array[kept_indexes] = True
    return ma_array





def load_and_get_centroids_from_step_volumes(step_path):
    """Centroids of brep cad file saved as step

    Args:
        step_path (str): the path to the file

    Returns:
        (tuple, list): entity_id, coordinates of entity center of mass as [x,y,z]
    """
    gmsh.initialize()
    gmsh.merge(str(step_path))

    entities = gmsh.model.getEntities()
    e_centroids = []
    e_ids = []
    logging.info(
        f"Getting centroids from stepfile {step_path.stem} containing {len(entities)} entities"
    )
    for i, e in enumerate(entities):
        if gmsh.model.getType(e[0], e[1]) == "Volume":
            e_ids.append((e[0], e[1]))
            e_cofmass = gmsh.model.occ.getCenterOfMass(e[0], e[1])
            e_centroids.append([e_cofmass[0], e_cofmass[1], e_cofmass[2]])
    logging.info(
        f"Output {len(e_ids)} centroids from volumes entities of stepfile {step_path.stem}"
    )
    gmsh.finalize()
    return e_ids, e_centroids


def load_and_match_points_to_cloud_rknn(
    in_points, cloud_path, distance_treshold, previsualization=False
):
    """Load a cloud and RKNN to specific distance treshold, return indexes and ids of volumes

    Args:
        points (_type_): array of points shaped [:,3]
        cloud_path (_type_): _description_
        distance_treshold (_type_): _description_

    Returns:
        _type_: _description_
    """
    in_points_cloud = o3d.geometry.PointCloud()
    in_points_cloud.points = o3d.utility.Vector3dVector(in_points)

    logging.info(f"-- Loading point cloud xyz coordinate from '{cloud_path.stem}' --")
    xyz_pts = np.loadtxt(fname=str(cloud_path), usecols=[0, 1, 2])
    pcd_pts = o3d.geometry.PointCloud()
    pcd_pts.points = o3d.utility.Vector3dVector(xyz_pts)
    logging.info(
        f"Loaded: {len(pcd_pts.points)} points, eg. {pcd_pts.points[np.random.randint(low=0, high=len(pcd_pts.points))]}"
    )

    kept_indexes = []
    pcdtree_pcd = o3d.geometry.KDTreeFlann(pcd_pts)
    for idx_cen, cen in enumerate(in_points_cloud.points):
        [k, idx, dist] = pcdtree_pcd.search_hybrid_vector_3d(
            cen, radius=distance_treshold, max_nn=1
        )
        if k:
            kept_indexes.append(idx_cen)
    logging.info(
        f"END Identified {len(kept_indexes)} matching volumes from '{cloud_path.stem}'"
    )

    if previsualization == True:
        pcd_pts.paint_uniform_color([1, 0.706, 0])

        e_kept_centroids = np.array(in_points)[kept_indexes]
        e_kept_centroids_cloud = o3d.geometry.PointCloud()
        e_kept_centroids_cloud.points = o3d.utility.Vector3dVector(e_kept_centroids)

        discarded_indexes = [
            iddx for iddx in range(len(e_ids)) if iddx not in kept_indexes
        ]
        e_discarded_centroids = np.array(in_points)[discarded_indexes]
        e_discarded_centroids_cloud = o3d.geometry.PointCloud()
        e_discarded_centroids_cloud.points = o3d.utility.Vector3dVector(
            e_discarded_centroids
        )
        e_discarded_centroids_cloud.paint_uniform_color([0.5, 0.5, 0.5])

        o3d.visualization.draw_geometries(
            [pcd_pts, e_kept_centroids_cloud, e_discarded_centroids_cloud]
        )

    return kept_indexes #, np.array(e_ids)[kept_indexes]
