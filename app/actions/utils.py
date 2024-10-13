import shapely.geometry


def generate_rectangle_cells(xmin, ymin, xmax, ymax, interval=0.3):
    # Create the grid coordinates for a rectangle.
    while xmin < xmax:
        ycur = ymin
        while ycur < ymax:
            yield (xmin, ycur, min(xmin + interval, xmax), min(ycur + interval, ymax))
            ycur += interval
        xmin += interval


def generate_geometry_fragments(geometry_collection, interval=0.5):

    geometry_collection = geometry_collection.simplify(tolerance=0.0005, preserve_topology=True)

    # It is possible for the simplify function will produce an invalid result.
    # Buffer it to produce a valid geometry.
    if not geometry_collection.is_valid:
        geometry_collection = geometry_collection.buffer(0)

    envelope = geometry_collection.envelope
    for xmin, ymin, xmax, ymax in generate_rectangle_cells(
        envelope.bounds[0],
        envelope.bounds[1],
        envelope.bounds[2],
        envelope.bounds[3],
        interval=interval,
    ):
        rectangle_shape = shapely.geometry.Polygon(
            [(xmin, ymin), (xmin, ymax), (xmax, ymax), (xmax, ymin)]
        )

        intersection = rectangle_shape.intersection(geometry_collection)
        if not intersection.is_empty:
            yield intersection if intersection.geometryType() == 'Polygon' else intersection.convex_hull

