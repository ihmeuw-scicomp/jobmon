export type TaskDependency = {
    id: number;
    name: string;
    status: string;
};
export type TaskDependenciesResponse = {
    up: [TaskDependency[]];
    down: [TaskDependency[]];
};
