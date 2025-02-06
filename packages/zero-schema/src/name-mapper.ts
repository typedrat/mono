export type NameMapping = 'snake' | 'none';

export type NameMapper = (jsName: string) => string;

function snakeCase(name: string): string {
  return name.replace(/([a-z])([A-Z])/g, '$1_$2').toLowerCase();
}

function identity(x: string): string {
  return x;
}

export function getNameMapper(nameMapping: NameMapping): NameMapper {
  switch (nameMapping) {
    case 'snake':
      return snakeCase;
    case 'none':
      return identity;
    default:
      return identity;
  }
}
