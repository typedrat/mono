/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/naming-convention */
import * as d3 from 'd3';
import {graphStratify} from 'd3-dag';
import {sugiyama, layeringSimplex, decrossOpt, coordGreedy} from 'd3-dag';

export type NodeVisual = {
  id: number;
  name: string;
  type: string;
};

export type EdgeVisual = {
  source: number;
  dest: number;
};

export type Graph = {
  nodes: NodeVisual[];
  edges: EdgeVisual[];
};

const nodeTypes = {
  Source: {color: '#6ede87', icon: '🔌'},
  Join: {color: '#ff9966', icon: '🔗'},
  Exists: {color: '#ffcc00', icon: '❓'},
  Take: {color: '#6495ed', icon: '✂️'},
  Skip: {color: '#ff6347', icon: '⏭️'},
  Filter: {color: '#ba55d3', icon: '🔍'},
  View: {color: '#ffb6c1', icon: '📺'},
  FanOut: {color: 'grey', icon: '🪭'},
  FanIn: {color: 'grey', icon: '🪭'},
};

class DAGVisualizer {
  private svg: d3.Selection<SVGSVGElement, unknown, HTMLElement, any>;
  private container: d3.Selection<HTMLDivElement, unknown, HTMLElement, any>;
  private width = 0; // Will be set dynamically
  private height = 0; // Will be set dynamically
  private nodeRadius = 30;
  private zoom: d3.ZoomBehavior<SVGSVGElement, unknown>;
  private g: d3.Selection<SVGGElement, unknown, HTMLElement, any>;
  private inputContainer: d3.Selection<
    HTMLDivElement,
    unknown,
    HTMLElement,
    any
  >;
  private textArea: d3.Selection<
    HTMLTextAreaElement,
    unknown,
    HTMLElement,
    any
  >;

  constructor(containerId: string) {
    // Create the main container with full height
    this.container = d3
      .select(`#${containerId}`)
      .append('div')
      .attr('class', 'dag-visualizer')
      .style('height', '100vh') // Full viewport height
      .style('display', 'flex')
      .style('flex-direction', 'column');

    // Create input container with fixed height
    this.inputContainer = this.container
      .append('div')
      .attr('class', 'input-container')
      .style('padding', '10px')
      .style('height', '140px'); // Fixed height for input area

    // Create textarea for JSON input
    this.textArea = this.inputContainer
      .append('textarea')
      .attr('placeholder', 'Paste your graph JSON here...')
      .style('width', '100%')
      .style('height', '100px');

    // Create button to visualize
    this.inputContainer
      .append('button')
      .text('Visualize')
      .style('padding', '8px 16px')
      .style('cursor', 'pointer')
      .on('click', () => this.handleVisualize());

    // Create zoom behavior
    this.zoom = d3
      .zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.1, 4])
      .on('zoom', event => {
        this.g.attr('transform', event.transform);
      });

    // Create SVG container that fills remaining space
    this.svg = this.container
      .append('svg')
      .style('flex', '1')
      .style('width', '100%')
      .style('min-height', '0')
      .call(this.zoom);

    // Update width and height to be dynamic
    const updateDimensions = () => {
      const bbox = this.svg.node()?.getBoundingClientRect();
      if (bbox) {
        this.width = bbox.width;
        this.height = bbox.height;
        this.svg
          .select('rect')
          .attr('width', this.width)
          .attr('height', this.height);
      }
    };

    // Listen for window resize
    window.addEventListener('resize', updateDimensions);
    // Initial dimension setup
    updateDimensions();

    // Create a group for the graph
    this.g = this.svg.append('g');

    // Add a defs section for markers
    const defs = this.svg.append('defs');

    // Define arrow marker
    defs
      .append('marker')
      .attr('id', 'arrowhead')
      .attr('viewBox', '0 -5 10 10')
      .attr('refX', 20)
      .attr('refY', 0)
      .attr('orient', 'auto')
      .attr('markerWidth', 6)
      .attr('markerHeight', 6)
      .append('path')
      .attr('d', 'M0,-5L10,0L0,5')
      .attr('fill', '#999');
  }

  private handleVisualize(): void {
    const jsonData = JSON.parse(this.textArea.property('value'));
    this.render(jsonData);
  }

  render(data: Graph): void {
    // Clear previous graph
    this.g.selectAll('*').remove();

    // Create stratify data with proper types
    const stratifyData = data.nodes.map(node => ({
      id: String(node.id),
      parentIds: data.edges
        .filter(edge => edge.dest === node.id)
        .map(edge => String(edge.source)),
      name: node.name,
      type: node.type,
    }));

    // Create the DAG layout
    const stratify = graphStratify();
    const dag = stratify(stratifyData);

    // Use sugiyama layout from d3-dag
    const layout = sugiyama()
      .nodeSize([this.nodeRadius * 4, this.nodeRadius * 7])
      .layering(layeringSimplex())
      .decross(decrossOpt())
      .coord(coordGreedy());

    // Apply the layout
    const {width, height} = layout(dag);

    // Create links
    this.g
      .selectAll('.link')
      .data(dag.links())
      .enter()
      .append('path')
      .attr('class', 'link')
      .attr('d', (d: any) => {
        const {points} = d;
        return d3
          .line()
          .x((p: [number, number]) => p[0])
          .y((p: [number, number]) => p[1])
          .curve(d3.curveCatmullRom)(points);
      })
      .attr('fill', 'none')
      .attr('stroke', '#999')
      .attr('stroke-width', 1.5)
      .attr('marker-end', 'url(#arrowhead)');

    // Create node groups
    const nodes = this.g
      .selectAll('.node')
      .data(dag.nodes())
      .enter()
      .append('g')
      .attr('class', 'node')
      .attr('transform', (d: any) => `translate(${d.x}, ${d.y})`)
      .call(
        d3
          .drag<SVGGElement, any>()
          .on('start', this.dragStarted.bind(this))
          .on('drag', this.dragged.bind(this))
          .on('end', this.dragEnded.bind(this)),
      );

    // Add node circles
    nodes
      .append('circle')
      .attr('r', this.nodeRadius)
      .attr('fill', (d: any) => {
        const nodeType = nodeTypes[d.data.type as keyof typeof nodeTypes];
        return nodeType ? nodeType.color : '#ccc';
      })
      .attr('stroke', '#666')
      .attr('stroke-width', 2);

    // Add node icons
    nodes
      .append('text')
      .attr('text-anchor', 'middle')
      .attr('dy', '0.3em')
      .text((d: any) => {
        const nodeType = nodeTypes[d.data.type as keyof typeof nodeTypes];
        return nodeType ? nodeType.icon : '?';
      })
      .style('font-size', '16px');

    // Add node labels
    nodes
      .append('text')
      .attr('text-anchor', 'middle')
      .attr('dy', this.nodeRadius * 2)
      .style('font-size', '12px')
      .style('font-weight', 'bold')
      .each(function (d: any) {
        const text = d3.select(this);

        // Add white background rectangle
        const bgPadding = 4;
        const parent = d3.select(this.parentElement!);
        parent
          .insert('rect', 'text')
          .attr('class', 'label-bg')
          .attr('fill', 'white')
          .attr('opacity', 0.9)
          .attr('rx', 4);

        // Add the full text
        text.text(d.data.name);

        // Adjust background rectangle size and position
        const bbox = text.node()?.getBBox();
        if (bbox) {
          parent
            .select('.label-bg')
            .attr('x', bbox.x - bgPadding)
            .attr('y', bbox.y - bgPadding)
            .attr('width', bbox.width + bgPadding * 2)
            .attr('height', bbox.height + bgPadding * 2);
        }
      });

    // Center the graph
    this.centerGraph(width, height);
  }

  private dragStarted(event: d3.D3DragEvent<SVGGElement, any, any>): void {
    d3.select(event.sourceEvent.currentTarget).raise().classed('active', true);
  }

  private dragged(event: d3.D3DragEvent<SVGGElement, any, any>, d: any): void {
    d.x = event.x;
    d.y = event.y;
    d3.select(event.sourceEvent.currentTarget).attr(
      'transform',
      `translate(${d.x}, ${d.y})`,
    );

    // Update connected links
    this.g.selectAll('.link').attr('d', (l: any) => {
      const {points} = l;
      return d3
        .line()
        .x((p: [number, number]) => p[0])
        .y((p: [number, number]) => p[1])
        .curve(d3.curveCatmullRom)(points);
    });
  }

  private dragEnded(event: d3.D3DragEvent<SVGGElement, any, any>): void {
    d3.select(event.sourceEvent.currentTarget).classed('active', false);
  }

  private centerGraph(width: number, height: number): void {
    const scale = 0.9 / Math.max(width / this.width, height / this.height);
    const translateX = this.width / 2 - (scale * width) / 2;
    const translateY = this.height / 2 - (scale * height) / 2;

    this.svg
      .transition()
      .duration(750)
      .call(
        this.zoom.transform,
        d3.zoomIdentity.translate(translateX, translateY).scale(scale),
      );
  }
}

// Initialize the visualizer
document.addEventListener('DOMContentLoaded', () => {
  const visualizer = new DAGVisualizer('app');

  // Example data to show on load
  const exampleData: Graph = {
    nodes: [
      {id: 24, name: 'Join(comments)', type: 'Join'},
      {id: 22, name: 'FanIn', type: 'FanIn'},
      {id: 20, name: 'Exists', type: 'Exists'},
      {id: 19, name: 'closed = literal', type: 'Filter'},
      {id: 18, name: 'id = literal', type: 'Filter'},
      {id: 17, name: 'FanOut', type: 'FanOut'},
      {id: 16, name: 'Join(zsubq_comments_0)', type: 'Join'},
      {id: 13, name: 'issue', type: 'Source'},
      {id: 15, name: 'Take(3)', type: 'Take'},
      {id: 14, name: 'comment', type: 'Source'},
      {id: 21, name: 'id = literal and ownerId = literal', type: 'Filter'},
      {id: 23, name: 'comment', type: 'Source'},
    ],
    edges: [
      {source: 22, dest: 24},
      {source: 20, dest: 22},
      {source: 19, dest: 20},
      {source: 18, dest: 19},
      {source: 17, dest: 18},
      {source: 16, dest: 17},
      {source: 13, dest: 16},
      {source: 15, dest: 16},
      {source: 14, dest: 15},
      {source: 21, dest: 22},
      {source: 17, dest: 21},
      {source: 23, dest: 24},
    ],
  };

  visualizer.render(exampleData);
});
